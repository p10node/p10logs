// Package api exposes the hub over HTTP: agent ingest, query, live tail, export,
// status, federation fan-out, and the embedded UI.
package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/p10node/p10logs/internal/auth"
	"github.com/p10node/p10logs/internal/federation"
	"github.com/p10node/p10logs/internal/index"
	"github.com/p10node/p10logs/internal/query"
	"github.com/p10node/p10logs/internal/store"
	"github.com/p10node/p10logs/internal/wire"
)

// Config for the API.
type Config struct {
	MaxBatchBytes   int64
	PerClusterBPS   int64
	MaxConcurrent   int
	TailMaxClients  int
	TailLinesPerSec int
	TailBuffer      int
	MetricsEnabled  bool
	Version         string
	Peers           *federation.Peers
}

// Server holds handlers.
type Server struct {
	cfg  Config
	st   *store.Store
	auth *auth.Auth
	ui   []byte
	sem  chan struct{}
	rlMu sync.Mutex
	rl   map[string]*bucket
	dec  *zstd.Decoder
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New builds the server.
func New(cfg Config, st *store.Store, a *auth.Auth, ui []byte) *Server {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 8
	}
	d, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(2))
	return &Server{cfg: cfg, st: st, auth: a, ui: ui, sem: make(chan struct{}, cfg.MaxConcurrent), rl: map[string]*bucket{}, dec: d}
}

// Handler returns the routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc(wire.PushPath, s.push)
	s.auth.Routes(mux)
	ui := http.NewServeMux()
	ui.HandleFunc("/api/v1/streams", s.streams)
	ui.HandleFunc("/api/v1/query", s.query)
	ui.HandleFunc("/api/v1/tail", s.tail)
	ui.HandleFunc("/api/v1/export", s.export)
	ui.HandleFunc("/api/v1/status", s.status)
	ui.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/index.html" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(s.ui)
	})
	if s.cfg.MetricsEnabled {
		mux.HandleFunc("/metrics", s.metrics)
	}
	mux.Handle("/", s.auth.RequireUI(ui))
	return mux
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// ---- ingest ----

func (s *Server) allow(cluster string, n int64) bool {
	if s.cfg.PerClusterBPS <= 0 {
		return true
	}
	s.rlMu.Lock()
	defer s.rlMu.Unlock()
	b := s.rl[cluster]
	now := time.Now()
	if b == nil {
		b = &bucket{tokens: float64(s.cfg.PerClusterBPS), last: now}
		s.rl[cluster] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * float64(s.cfg.PerClusterBPS)
	if b.tokens > float64(s.cfg.PerClusterBPS)*2 {
		b.tokens = float64(s.cfg.PerClusterBPS) * 2
	}
	b.last = now
	if b.tokens < 0 {
		return false
	}
	b.tokens -= float64(n)
	return true
}

func (s *Server) push(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	cluster := r.Header.Get(wire.HdrCluster)
	if !s.auth.CheckIngest(r, cluster) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	node := r.Header.Get(wire.HdrNode)
	if cluster == "" || strings.ContainsAny(cluster, "/\\") {
		http.Error(w, "missing or invalid "+wire.HdrCluster, http.StatusBadRequest)
		return
	}
	s.st.Heartbeat(cluster, node, r.Header.Get(wire.HdrAgent), r.Header.Get(wire.HdrStats))
	if r.ContentLength == 0 {
		w.WriteHeader(204)
		return
	}
	var body []byte
	var err error
	if r.Header.Get("Content-Encoding") == "zstd" {
		comp, rerr := io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxBatchBytes+1))
		if rerr != nil {
			http.Error(w, rerr.Error(), http.StatusBadRequest)
			return
		}
		body, err = s.dec.DecodeAll(comp, make([]byte, 0, len(comp)*4))
		if err != nil {
			http.Error(w, "zstd: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		body, err = io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxBatchBytes+1))
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if int64(len(body)) > s.cfg.MaxBatchBytes {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}
	if !s.allow(cluster, int64(len(body))) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "cluster rate limit", http.StatusTooManyRequests)
		return
	}
	res, err := s.st.Ingest(cluster, node, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if res.Deduped > 0 {
		w.WriteHeader(202)
		return
	}
	w.WriteHeader(204)
}

// ---- selectors, time, access ----

func selectorFrom(q url.Values, localSIDs []string) index.Selector {
	sel := index.Selector{Cluster: q.Get("cluster"), Namespace: q.Get("namespace"), Pod: q.Get("pod"), Container: q.Get("container"), UID: q.Get("uid")}
	for _, x := range localSIDs {
		if id, err := strconv.ParseInt(x, 10, 64); err == nil {
			sel.IDs = append(sel.IDs, id)
		}
	}
	if len(localSIDs) > 0 && len(sel.IDs) == 0 {
		sel.IDs = []int64{-1} // sids were given but none parse: match nothing
	}
	if v := q.Get("since"); v != "" {
		sel.Since = parseTime(v, 0)
	}
	for _, kv := range q["label"] { // label=app:api  or  label=app=api
		if i := strings.IndexAny(kv, ":="); i > 0 {
			if sel.Labels == nil {
				sel.Labels = map[string]string{}
			}
			sel.Labels[kv[:i]] = kv[i+1:]
		}
	}
	return sel
}

// parseTime accepts RFC3339, "now", relative "-15m", unix seconds/millis/nanos.
func parseTime(v string, def int64) int64 {
	v = strings.TrimSpace(v)
	if v == "" {
		return def
	}
	if v == "now" {
		return time.Now().UnixNano()
	}
	if strings.HasPrefix(v, "-") {
		if d, err := time.ParseDuration(v[1:]); err == nil {
			return time.Now().Add(-d).UnixNano()
		}
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		switch {
		case n > 1e17:
			return n
		case n > 1e14:
			return n * 1e3
		case n > 1e11:
			return n * 1e6
		default:
			return n * 1e9
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UnixNano()
	}
	return def
}

// scope applies the caller's role (namespace/cluster allow-lists) to a selector.
func (s *Server) scope(r *http.Request, sel index.Selector) index.Selector {
	if role := s.auth.Role(r); role != nil {
		sel.Allow = role.Allow
	}
	return sel
}

func hopsOK(w http.ResponseWriter, r *http.Request) bool {
	if federation.Hops(r) > federation.MaxHops {
		jsonErr(w, http.StatusLoopDetected, "federation loop")
		return false
	}
	return true
}

// peerQuery builds the query string forwarded to a peer (sid rewritten to that peer's ids).
func peerQuery(q url.Values, sids []string) url.Values {
	out := url.Values{}
	for k, v := range q {
		if k != "sid" {
			out[k] = v
		}
	}
	if len(sids) > 0 {
		out["sid"] = []string{strings.Join(sids, ",")}
	}
	return out
}

// ---- streams ----

type treeCtr struct {
	Name   string `json:"name"`
	ID     string `json:"id"`
	LastTS int64  `json:"last_ts"`
	Ended  bool   `json:"ended"`
}
type treePod struct {
	Name       string            `json:"name"`
	UID        string            `json:"uid"`
	Node       string            `json:"node,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Restarts   int               `json:"restarts"`
	LastTS     int64             `json:"last_ts"`
	Live       bool              `json:"live"`
	Containers []*treeCtr        `json:"containers"`
}
type treeNS struct {
	Name string     `json:"name"`
	Pods []*treePod `json:"pods"`
}
type treeCluster struct {
	Name       string    `json:"name"`
	Hub        string    `json:"hub,omitempty"`
	Namespaces []*treeNS `json:"namespaces"`
}
type treeResp struct {
	Clusters    []*treeCluster `json:"clusters"`
	Count       int            `json:"count"`
	PeersFailed []string       `json:"peers_failed,omitempty"`
}

func (s *Server) localTree(ctx context.Context, sel index.Selector) (treeResp, error) {
	sts, err := s.st.Idx.ListStreams(ctx, sel, 0)
	if err != nil {
		return treeResp{}, err
	}
	cl := map[string]*treeCluster{}
	ns := map[string]*treeNS{}
	pods := map[string]*treePod{}
	for _, st := range sts {
		c := cl[st.Cluster]
		if c == nil {
			c = &treeCluster{Name: st.Cluster}
			cl[st.Cluster] = c
		}
		nk := st.Cluster + "/" + st.Namespace
		n := ns[nk]
		if n == nil {
			n = &treeNS{Name: st.Namespace}
			ns[nk] = n
			c.Namespaces = append(c.Namespaces, n)
		}
		pk := nk + "/" + st.UID
		p := pods[pk]
		if p == nil {
			p = &treePod{Name: st.Pod, UID: st.UID, Node: st.Node, Live: true}
			pods[pk] = p
			n.Pods = append(n.Pods, p)
		}
		if st.Restarts > p.Restarts {
			p.Restarts = st.Restarts
		}
		if st.LastTS > p.LastTS {
			p.LastTS = st.LastTS
		}
		if st.Ended {
			p.Live = false
		}
		if len(st.Labels) > 0 && p.Labels == nil {
			p.Labels = st.Labels
		}
		p.Containers = append(p.Containers, &treeCtr{Name: st.Container, ID: strconv.FormatInt(st.ID, 10), LastTS: st.LastTS, Ended: st.Ended})
	}
	out := treeResp{Clusters: []*treeCluster{}, Count: len(sts)}
	for _, c := range cl {
		sort.Slice(c.Namespaces, func(i, j int) bool { return c.Namespaces[i].Name < c.Namespaces[j].Name })
		for _, n := range c.Namespaces {
			sort.Slice(n.Pods, func(i, j int) bool { return n.Pods[i].Name < n.Pods[j].Name })
			for _, p := range n.Pods {
				sort.Slice(p.Containers, func(i, j int) bool { return p.Containers[i].Name < p.Containers[j].Name })
			}
		}
		out.Clusters = append(out.Clusters, c)
	}
	sort.Slice(out.Clusters, func(i, j int) bool { return out.Clusters[i].Name < out.Clusters[j].Name })
	return out, nil
}

func (s *Server) streams(w http.ResponseWriter, r *http.Request) {
	if !hopsOK(w, r) {
		return
	}
	q := r.URL.Query()
	routes := federation.Route(q["sid"])
	hasSID := len(q["sid"]) > 0
	var out treeResp
	if !hasSID || len(routes[""]) > 0 {
		t, err := s.localTree(r.Context(), s.scope(r, selectorFrom(q, routes[""])))
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		out = t
	}
	if s.cfg.Peers.Enabled() {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, p := range s.cfg.Peers.List {
			if hasSID && len(routes[p.Name]) == 0 {
				continue
			}
			wg.Add(1)
			go func(p federation.Peer) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
				defer cancel()
				resp, err := s.cfg.Peers.Get(ctx, p, "/api/v1/streams", peerQuery(q, routes[p.Name]), federation.Hops(r), "application/json")
				var t treeResp
				if err == nil {
					err = json.NewDecoder(resp.Body).Decode(&t)
					resp.Body.Close()
				}
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					out.PeersFailed = append(out.PeersFailed, p.Name)
					return
				}
				for _, c := range t.Clusters {
					if c.Hub == "" {
						c.Hub = p.Name
					} else {
						c.Hub = p.Name + "/" + c.Hub
					}
					for _, n := range c.Namespaces {
						for _, pod := range n.Pods {
							for _, ct := range pod.Containers {
								ct.ID = p.Name + "/" + ct.ID
							}
						}
					}
					out.Clusters = append(out.Clusters, c)
				}
				out.Count += t.Count
			}(p)
		}
		wg.Wait()
		sort.SliceStable(out.Clusters, func(i, j int) bool {
			return out.Clusters[i].Hub+out.Clusters[i].Name < out.Clusters[j].Hub+out.Clusters[j].Name
		})
	}
	if out.Clusters == nil {
		out.Clusters = []*treeCluster{}
	}
	writeJSON(w, out)
}

// ---- query ----

type apiLine struct {
	SID string `json:"sid"`
	T   int64  `json:"t"`
	E   bool   `json:"e,omitempty"`
	M   string `json:"m"`
	R   int    `json:"r,omitempty"`
}

type apiResult struct {
	Lines       []apiLine `json:"lines"`
	Chunks      int       `json:"chunks"`
	Scanned     int64     `json:"scanned_bytes"`
	Millis      int64     `json:"ms"`
	Truncated   bool      `json:"truncated"`
	Skipped     int       `json:"skipped_chunks"`
	Next        int64     `json:"next,omitempty"`
	PeersFailed []string  `json:"peers_failed,omitempty"`
}

func toAPI(res store.Result, prefix string) apiResult {
	out := apiResult{Lines: make([]apiLine, len(res.Lines)), Chunks: res.Chunks, Scanned: res.Scanned, Millis: res.Millis, Truncated: res.Truncated, Skipped: res.Skipped, Next: res.Next}
	for i, l := range res.Lines {
		out.Lines[i] = apiLine{SID: prefix + strconv.FormatInt(l.SID, 10), T: l.TS, E: l.Stderr, M: l.Msg, R: l.Restart}
	}
	return out
}

func (s *Server) acquire(ctx context.Context) bool {
	select {
	case s.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(10 * time.Second):
		return false
	}
}

// federatedQuery runs the query locally and on peers, merging newest-first.
func (s *Server) federatedQuery(r *http.Request, limit int) (apiResult, error) {
	q := r.URL.Query()
	start := parseTime(q.Get("start"), time.Now().Add(-time.Hour).UnixNano())
	end := parseTime(q.Get("end"), time.Now().Add(time.Minute).UnixNano())
	routes := federation.Route(q["sid"])
	hasSID := len(q["sid"]) > 0
	t0 := time.Now()
	var mu sync.Mutex
	var wg sync.WaitGroup
	merged := apiResult{Lines: []apiLine{}}
	add := func(res apiResult) {
		mu.Lock()
		defer mu.Unlock()
		merged.Lines = append(merged.Lines, res.Lines...)
		merged.Chunks += res.Chunks
		merged.Scanned += res.Scanned
		merged.Skipped += res.Skipped
		merged.Truncated = merged.Truncated || res.Truncated
		merged.PeersFailed = append(merged.PeersFailed, res.PeersFailed...)
	}
	var localErr error
	if !hasSID || len(routes[""]) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.st.Query(r.Context(), s.scope(r, selectorFrom(q, routes[""])), start, end, query.Compile(q.Get("q")), limit)
			if err != nil {
				localErr = err
				return
			}
			add(toAPI(res, ""))
		}()
	}
	if s.cfg.Peers.Enabled() {
		for _, p := range s.cfg.Peers.List {
			if hasSID && len(routes[p.Name]) == 0 {
				continue
			}
			wg.Add(1)
			go func(p federation.Peer) {
				defer wg.Done()
				pq := peerQuery(q, routes[p.Name])
				pq.Set("limit", strconv.Itoa(limit))
				resp, err := s.cfg.Peers.Get(r.Context(), p, "/api/v1/query", pq, federation.Hops(r), "application/json")
				var res apiResult
				if err == nil {
					err = json.NewDecoder(resp.Body).Decode(&res)
					resp.Body.Close()
				}
				if err != nil {
					add(apiResult{PeersFailed: []string{p.Name}})
					return
				}
				for i := range res.Lines {
					res.Lines[i].SID = p.Name + "/" + res.Lines[i].SID
				}
				for i := range res.PeersFailed {
					res.PeersFailed[i] = p.Name + "/" + res.PeersFailed[i]
				}
				add(res)
			}(p)
		}
	}
	wg.Wait()
	if localErr != nil {
		return apiResult{}, localErr
	}
	sort.SliceStable(merged.Lines, func(i, j int) bool { return merged.Lines[i].T > merged.Lines[j].T })
	if len(merged.Lines) > limit {
		merged.Lines = merged.Lines[:limit]
		merged.Truncated = true
	}
	if len(merged.Lines) > 0 {
		merged.Next = merged.Lines[len(merged.Lines)-1].T - 1
	}
	merged.Millis = time.Since(t0).Milliseconds()
	return merged, nil
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	if !hopsOK(w, r) {
		return
	}
	if !s.acquire(r.Context()) {
		jsonErr(w, http.StatusServiceUnavailable, "too many concurrent queries")
		return
	}
	defer func() { <-s.sem }()
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	res, err := s.federatedQuery(r, limit)
	if err != nil {
		jsonErr(w, 500, err.Error())
		return
	}
	writeJSON(w, res)
}

// ---- export ----

type exportLine struct {
	T       string `json:"t"`
	Stream  string `json:"stream"`
	Stderr  bool   `json:"stderr"`
	Restart int    `json:"restart"`
	M       string `json:"m"`
	ts      int64
}

func (s *Server) export(w http.ResponseWriter, r *http.Request) {
	if !hopsOK(w, r) {
		return
	}
	if !s.acquire(r.Context()) {
		jsonErr(w, http.StatusServiceUnavailable, "too many concurrent queries")
		return
	}
	defer func() { <-s.sem }()
	q := r.URL.Query()
	start := parseTime(q.Get("start"), time.Now().Add(-time.Hour).UnixNano())
	end := parseTime(q.Get("end"), time.Now().UnixNano())
	routes := federation.Route(q["sid"])
	hasSID := len(q["sid"]) > 0
	var lines []exportLine
	truncated := false
	if !hasSID || len(routes[""]) > 0 {
		sel := s.scope(r, selectorFrom(q, routes[""]))
		res, err := s.st.Query(r.Context(), sel, start, end, query.Compile(q.Get("q")), 1_000_000)
		if err != nil {
			jsonErr(w, 500, err.Error())
			return
		}
		names := map[int64]string{}
		if sts, err := s.st.Idx.ListStreams(r.Context(), sel, 0); err == nil {
			for _, st := range sts {
				names[st.ID] = st.Cluster + "/" + st.Namespace + "/" + st.Pod + "/" + st.Container
			}
		}
		for _, l := range res.Lines {
			lines = append(lines, exportLine{T: time.Unix(0, l.TS).UTC().Format(time.RFC3339Nano), Stream: names[l.SID], Stderr: l.Stderr, Restart: l.Restart, M: l.Msg, ts: l.TS})
		}
		truncated = res.Truncated
	}
	if s.cfg.Peers.Enabled() {
		for _, p := range s.cfg.Peers.List {
			if hasSID && len(routes[p.Name]) == 0 {
				continue
			}
			pq := peerQuery(q, routes[p.Name])
			pq.Set("format", "ndjson")
			resp, err := s.cfg.Peers.Get(r.Context(), p, "/api/v1/export", pq, federation.Hops(r), "")
			if err != nil {
				continue
			}
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 1<<20), 8<<20)
			for sc.Scan() {
				var l exportLine
				if json.Unmarshal(sc.Bytes(), &l) != nil {
					continue
				}
				if t, err := time.Parse(time.RFC3339Nano, l.T); err == nil {
					l.ts = t.UnixNano()
				}
				l.Stream = p.Name + ":" + l.Stream
				lines = append(lines, l)
			}
			resp.Body.Close()
		}
	}
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].ts < lines[j].ts }) // oldest first
	format := q.Get("format")
	ext := "txt"
	if format == "ndjson" {
		ext = "ndjson"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="p10logs-%s.%s"`, time.Now().UTC().Format("20060102-150405"), ext))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	bw := bufio.NewWriterSize(w, 64<<10)
	defer bw.Flush()
	enc := json.NewEncoder(bw)
	for _, l := range lines {
		if format == "ndjson" {
			enc.Encode(l)
			continue
		}
		fmt.Fprintf(bw, "%s %s %s\n", l.T, l.Stream, l.M)
	}
	if truncated {
		fmt.Fprintln(bw, "# truncated: narrow the time range or filter")
	}
}

// ---- tail ----

func (s *Server) tail(w http.ResponseWriter, r *http.Request) {
	if !hopsOK(w, r) {
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	if s.st.Bus.Count() >= s.cfg.TailMaxClients {
		jsonErr(w, http.StatusServiceUnavailable, "too many tail clients")
		return
	}
	q := r.URL.Query()
	routes := federation.Route(q["sid"])
	hasSID := len(q["sid"]) > 0
	f := query.Compile(q.Get("q"))
	sel := s.scope(r, selectorFrom(q, routes[""]))
	local := !hasSID || len(routes[""]) > 0

	var mu sync.RWMutex
	ids := map[int64]bool{}
	refresh := func() {
		if !local {
			return
		}
		sts, err := s.st.Idx.ListStreams(r.Context(), sel, 0)
		if err != nil {
			return
		}
		m := map[int64]bool{}
		for _, st := range sts {
			m[st.ID] = true
		}
		mu.Lock()
		ids = m
		mu.Unlock()
	}
	refresh()
	sub := s.st.Bus.Subscribe(s.cfg.TailBuffer, func(id int64) bool { mu.RLock(); defer mu.RUnlock(); return ids[id] })
	defer s.st.Bus.Unsubscribe(sub)

	// peer tails feed pre-serialised events into one channel
	peerEv := make(chan []byte, s.cfg.TailBuffer)
	var peerDropped int64
	var pdMu sync.Mutex
	if s.cfg.Peers.Enabled() {
		for _, p := range s.cfg.Peers.List {
			if hasSID && len(routes[p.Name]) == 0 {
				continue
			}
			go func(p federation.Peer) {
				resp, err := s.cfg.Peers.Get(r.Context(), p, "/api/v1/tail", peerQuery(q, routes[p.Name]), federation.Hops(r), "text/event-stream")
				if err != nil {
					return
				}
				defer resp.Body.Close()
				sc := bufio.NewScanner(resp.Body)
				sc.Buffer(make([]byte, 64<<10), 4<<20)
				prefix := []byte(`"sid":"`)
				repl := []byte(`"sid":"` + p.Name + "/")
				for sc.Scan() {
					line := sc.Bytes()
					if !bytes.HasPrefix(line, []byte("data: ")) {
						continue
					}
					ev := bytes.Replace(append([]byte(nil), line[6:]...), prefix, repl, 1)
					select {
					case peerEv <- ev:
					default:
						pdMu.Lock()
						peerDropped++
						pdMu.Unlock()
					}
				}
			}(p)
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	fmt.Fprint(w, ": p10logs tail\n\n")
	fl.Flush()

	perTick := s.cfg.TailLinesPerSec / 10
	if perTick < 1 {
		perTick = 1
	}
	tick := time.NewTicker(100 * time.Millisecond)
	ping := time.NewTicker(15 * time.Second)
	rf := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	defer ping.Stop()
	defer rf.Stop()
	var lastDropped int64
	buf := bufio.NewWriterSize(w, 32<<10)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(buf, ": ping\n\n")
			buf.Flush()
			fl.Flush()
		case <-rf.C:
			refresh()
		case <-tick.C:
			sent := 0
			for n := 0; n < perTick; n++ {
				select {
				case e := <-sub.Ch:
					if !f.Match([]byte(e.Msg)) {
						continue
					}
					b, _ := json.Marshal(apiLine{SID: strconv.FormatInt(e.StreamID, 10), T: e.TS, E: e.Stderr, M: e.Msg, R: e.Restart})
					buf.WriteString("data: ")
					buf.Write(b)
					buf.WriteString("\n\n")
					sent++
				default:
					n = perTick
				}
			}
			for n := 0; n < perTick; n++ {
				select {
				case ev := <-peerEv:
					buf.WriteString("data: ")
					buf.Write(ev)
					buf.WriteString("\n\n")
					sent++
				default:
					n = perTick
				}
			}
			pdMu.Lock()
			d := sub.Dropped.Load() + peerDropped
			pdMu.Unlock()
			if d > lastDropped {
				fmt.Fprintf(buf, "data: {\"dropped\":%d}\n\n", d-lastDropped)
				lastDropped = d
				sent++
			}
			if sent > 0 {
				buf.Flush()
				fl.Flush()
			}
		}
	}
}

// ---- status ----

func (s *Server) localStatus() map[string]any {
	st, _ := s.st.Idx.Stats()
	memB, memN, walB := s.st.MemStats()
	return map[string]any{
		"hub": map[string]any{
			"version": s.cfg.Version, "uptime_s": int64(s.st.Uptime().Seconds()),
			"disk_used": s.st.DiskUsed(), "disk_cap": s.st.DiskCap(),
			"ingest_bytes_per_s": s.st.RateBytes.Load(), "ingest_lines_per_s": s.st.RateLines.Load(),
			"ingest_bytes_total": s.st.InBytes.Load(), "ingest_lines_total": s.st.InLines.Load(), "deduped_lines": s.st.Deduped.Load(),
			"streams": st.Streams, "streams_live": st.Live, "chunks": st.Chunks, "open_chunks": s.st.OpenChunks(),
			"memtable_bytes": memB, "memtable_streams": memN, "wal_bytes": walB,
			"remote_chunks": st.RemoteChunks, "remote_only_chunks": st.RemoteOnlyChunks,
			"tail_clients": s.st.Bus.Count(), "oldest_day": first(st.Days), "days": len(st.Days),
		},
		"agents": s.st.Agents(),
	}
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if !hopsOK(w, r) {
		return
	}
	out := s.localStatus()
	if s.cfg.Peers.Enabled() {
		var peers []map[string]any
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, p := range s.cfg.Peers.List {
			wg.Add(1)
			go func(p federation.Peer) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
				defer cancel()
				entry := map[string]any{"name": p.Name, "url": p.URL, "ok": false}
				resp, err := s.cfg.Peers.Get(ctx, p, "/api/v1/status", nil, federation.Hops(r), "application/json")
				if err == nil {
					var st map[string]any
					if json.NewDecoder(resp.Body).Decode(&st) == nil {
						entry["ok"] = true
						entry["hub"] = st["hub"]
						entry["agents"] = st["agents"]
						entry["peers"] = st["peers"]
					}
					resp.Body.Close()
				} else {
					entry["error"] = err.Error()
				}
				mu.Lock()
				peers = append(peers, entry)
				mu.Unlock()
			}(p)
		}
		wg.Wait()
		sort.Slice(peers, func(i, j int) bool { return peers[i]["name"].(string) < peers[j]["name"].(string) })
		out["peers"] = peers
	}
	writeJSON(w, out)
}

func first(a []string) string {
	if len(a) == 0 {
		return ""
	}
	return a[0]
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	st, _ := s.st.Idx.Stats()
	memB, _, walB := s.st.MemStats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "p10logs_ingest_bytes_total %d\np10logs_ingest_lines_total %d\np10logs_deduped_lines_total %d\np10logs_streams %d\np10logs_chunks %d\np10logs_disk_used_bytes %d\np10logs_memtable_bytes %d\np10logs_wal_bytes %d\np10logs_tail_clients %d\n",
		s.st.InBytes.Load(), s.st.InLines.Load(), s.st.Deduped.Load(), st.Streams, st.Chunks, s.st.DiskUsed(), memB, walB, s.st.Bus.Count())
}
