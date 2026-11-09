package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/p10node/p10logs/internal/auth"
	"github.com/p10node/p10logs/internal/federation"
	"github.com/p10node/p10logs/internal/store"
	"github.com/p10node/p10logs/internal/wire"
)

// ---- harness ----

const (
	base  = int64(1_700_000_000_000_000_000) // 2023-11-14T22:13:20Z: well inside start=0&end=endNanos
	endNS = "2000000000000000000"            // parseTime: > 1e17 is read as nanoseconds
)

type hub struct {
	srv *httptest.Server
	st  *store.Store
	s   *Server
}

func newHub(t *testing.T, ac auth.Config, cfg Config) *hub {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(store.Config{Dir: t.TempDir()}, log)
	if err != nil {
		t.Fatal(err)
	}
	a, err := auth.New(context.Background(), ac)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = 1 << 20
	}
	if cfg.TailMaxClients == 0 {
		cfg.TailMaxClients = 4
	}
	if cfg.TailLinesPerSec == 0 {
		cfg.TailLinesPerSec = 1000
	}
	if cfg.TailBuffer == 0 {
		cfg.TailBuffer = 256
	}
	if cfg.Version == "" {
		cfg.Version = "test"
	}
	s := New(cfg, st, a, []byte("<html>p10logs ui</html>"))
	srv := httptest.NewServer(s.Handler()) // 127.0.0.1
	t.Cleanup(func() { srv.Close(); st.Close() })
	return &hub{srv: srv, st: st, s: s}
}

// batch builds one push section: lines get timestamps base + (off+i) seconds.
func batch(key, file string, off, end int64, restart int, labels map[string]string, lines ...string) string {
	var b strings.Builder
	h, _ := json.Marshal(wire.Section{Key: key, File: file, Off: [2]int64{off, end}, Restart: restart, Labels: labels}) // "k" first: the hub sniffs it
	b.Write(h)
	b.WriteByte('\n')
	for i, l := range lines {
		e, _ := json.Marshal(wire.Entry{T: base + (off+int64(i))*1e9, S: "o", M: l})
		b.Write(e)
		b.WriteByte('\n')
	}
	return b.String()
}

func (h *hub) do(t *testing.T, method, path, body string, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (h *hub) push(t *testing.T, token, cluster, body string, extra map[string]string) int {
	t.Helper()
	hdr := map[string]string{wire.HdrCluster: cluster, wire.HdrNode: "n1"}
	if token != "" {
		hdr["Authorization"] = "Bearer " + token
	}
	for k, v := range extra {
		hdr[k] = v
	}
	resp := h.do(t, http.MethodPost, wire.PushPath, body, hdr)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func (h *hub) mustPush(t *testing.T, cluster, body string) {
	t.Helper()
	if c := h.push(t, "tok", cluster, body, nil); c != 204 {
		t.Fatalf("push %s: %d", cluster, c)
	}
}

func (h *hub) getJSON(t *testing.T, path string, hdr map[string]string, v any) int {
	t.Helper()
	resp := h.do(t, http.MethodGet, path, "", hdr)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if v != nil && resp.StatusCode == 200 {
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatalf("%s: %v: %s", path, err, b)
		}
	}
	return resp.StatusCode
}

func (h *hub) getText(t *testing.T, path string, hdr map[string]string) (int, http.Header, string) {
	t.Helper()
	resp := h.do(t, http.MethodGet, path, "", hdr)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

func (h *hub) streams(t *testing.T, qs string, hdr map[string]string) treeResp {
	t.Helper()
	var out treeResp
	if c := h.getJSON(t, "/api/v1/streams?"+qs, hdr, &out); c != 200 {
		t.Fatalf("streams?%s: %d", qs, c)
	}
	return out
}

func (h *hub) query(t *testing.T, qs string, hdr map[string]string) apiResult {
	t.Helper()
	var out apiResult
	if c := h.getJSON(t, "/api/v1/query?start=0&end="+endNS+"&"+qs, hdr, &out); c != 200 {
		t.Fatalf("query?%s: %d", qs, c)
	}
	return out
}

func (t treeResp) ids() map[string]string { // "cluster/ns/pod/ctr" → sid
	out := map[string]string{}
	for _, c := range t.Clusters {
		for _, n := range c.Namespaces {
			for _, p := range n.Pods {
				for _, ct := range p.Containers {
					out[c.Name+"/"+n.Name+"/"+p.Name+"/"+ct.Name] = ct.ID
				}
			}
		}
	}
	return out
}

func msgs(lines []apiLine) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.M
	}
	return out
}

func basic(user, pass string) map[string]string {
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth(user, pass)
	return map[string]string{"Authorization": req.Header.Get("Authorization")}
}

func bearer(tok string) map[string]string { return map[string]string{"Authorization": "Bearer " + tok} }

var openAuth = auth.Config{Mode: "none", IngestTokens: []string{"tok"}}

// seed writes the fixture used by several tests: two clusters, two namespaces, labels, a restart.
func seed(t *testing.T, h *hub, hdr map[string]string) map[string]string {
	t.Helper()
	h.mustPush(t, "prod", batch("payments/api-1/uid-1/app", "f1", 0, 100, 0, map[string]string{"app": "api", "team": "pay"}, "hello world", "error: boom"))
	h.mustPush(t, "prod", batch("payments/api-1/uid-1/sidecar", "f2", 200, 250, 0, nil, "proxy ok"))
	h.mustPush(t, "prod", batch("db/postgres-0/uid-9/db", "f3", 300, 310, 2, nil, "ready to accept connections"))
	h.mustPush(t, "edge-1", batch("payments/api-2/uid-2/app", "f4", 400, 410, 0, nil, "edge line"))
	return h.streams(t, "", hdr).ids()
}

// ---- ingest ----

func TestHealthAndPush(t *testing.T) {
	h := newHub(t, auth.Config{Mode: "none", IngestTokens: []string{"tok"}, ClusterTokens: []auth.ClusterToken{{Token: "ctok", Clusters: []string{"edge-*"}}}}, Config{MaxBatchBytes: 2048})
	for _, p := range []string{"/healthz", "/readyz"} {
		if c, _, _ := h.getText(t, p, nil); c != 204 {
			t.Fatalf("%s: %d", p, c)
		}
	}
	if resp := h.do(t, http.MethodGet, wire.PushPath, "", nil); resp.StatusCode != 405 {
		t.Fatalf("GET push: %d", resp.StatusCode)
	}
	if c := h.push(t, "", "prod", "", nil); c != 401 {
		t.Fatalf("no token: %d", c)
	}
	if c := h.push(t, "wrong", "prod", "", nil); c != 401 {
		t.Fatalf("bad token: %d", c)
	}
	if c := h.push(t, "tok", "", "", nil); c != 400 {
		t.Fatalf("missing cluster: %d", c)
	}
	if c := h.push(t, "tok", "a/b", "", nil); c != 400 {
		t.Fatalf("slash in cluster: %d", c)
	}
	// heartbeat only (empty body) still registers the agent with its stats
	if c := h.push(t, "tok", "prod", "", map[string]string{wire.HdrAgent: "1.2.3", wire.HdrStats: "files=3;lag_bytes=10;spool_bytes=5;dropped=1;rss=12345;junk"}); c != 204 {
		t.Fatalf("heartbeat: %d", c)
	}
	var st struct {
		Hub    map[string]any    `json:"hub"`
		Agents []store.AgentInfo `json:"agents"`
	}
	h.getJSON(t, "/api/v1/status", nil, &st)
	if len(st.Agents) != 1 || st.Agents[0].Cluster != "prod" || st.Agents[0].Node != "n1" || st.Agents[0].Version != "1.2.3" || st.Agents[0].Files != 3 || st.Agents[0].RSS != 12345 || st.Agents[0].Dropped != 1 {
		t.Fatalf("agents: %+v", st.Agents)
	}
	if st.Hub["version"] != "test" || st.Hub["streams"].(float64) != 0 {
		t.Fatalf("hub status: %+v", st.Hub)
	}
	body := batch("payments/api-1/uid-1/app", "f1", 0, 100, 0, nil, "one", "two")
	if c := h.push(t, "tok", "prod", body, nil); c != 204 {
		t.Fatalf("push: %d", c)
	}
	if c := h.push(t, "tok", "prod", body, nil); c != 202 {
		t.Fatalf("resend should be 202 (deduped): %d", c)
	}
	// zstd-compressed batch
	enc, _ := zstd.NewWriter(nil)
	comp := enc.EncodeAll([]byte(batch("payments/api-1/uid-1/app", "f1", 100, 200, 0, nil, "three")), nil)
	if c := h.push(t, "tok", "prod", string(comp), map[string]string{"Content-Encoding": "zstd"}); c != 204 {
		t.Fatalf("zstd push: %d", c)
	}
	if c := h.push(t, "tok", "prod", "not zstd at all", map[string]string{"Content-Encoding": "zstd"}); c != 400 {
		t.Fatalf("bad zstd: %d", c)
	}
	if c := h.push(t, "tok", "prod", strings.Repeat("x", 3000), nil); c != 413 {
		t.Fatalf("oversize: %d", c)
	}
	if c := h.push(t, "tok", "prod", `{"k":"ns/pod/uid/c","f":"f","o":[0,1],"r":0}`+"\n"+`{"t":1,"s":"o","m":`+"\n", nil); c != 500 {
		t.Fatalf("malformed entry: %d", c)
	}
	// cluster-scoped tokens
	if c := h.push(t, "ctok", "edge-1", "", nil); c != 204 {
		t.Fatalf("cluster token for its cluster: %d", c)
	}
	if c := h.push(t, "ctok", "prod", "", nil); c != 401 {
		t.Fatalf("cluster token for another cluster: %d", c)
	}
	h.getJSON(t, "/api/v1/status", nil, &st)
	if st.Hub["ingest_lines_total"].(float64) != 3 || st.Hub["deduped_lines"].(float64) != 2 || st.Hub["streams"].(float64) != 1 {
		t.Fatalf("hub counters: %+v", st.Hub)
	}
}

func TestClusterRateLimit(t *testing.T) {
	h := newHub(t, openAuth, Config{PerClusterBPS: 10})
	body := batch("ns/pod/uid/c", "f", 0, 10, 0, nil, "a line that is longer than the per-second budget")
	if c := h.push(t, "tok", "prod", body, nil); c != 204 {
		t.Fatalf("first push: %d", c)
	}
	resp := h.do(t, http.MethodPost, wire.PushPath, batch("ns/pod/uid/c", "f", 10, 20, 0, nil, "x"), map[string]string{"Authorization": "Bearer tok", wire.HdrCluster: "prod"})
	resp.Body.Close()
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("second push: %d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if c := h.push(t, "tok", "other", body, nil); c != 204 {
		t.Fatalf("other cluster has its own bucket: %d", c)
	}
}

// ---- streams / query / export ----

func TestStreamsQueryExport(t *testing.T) {
	h := newHub(t, openAuth, Config{})
	ids := seed(t, h, nil)
	tr := h.streams(t, "", nil)
	if tr.Count != 4 || len(tr.Clusters) != 2 || tr.Clusters[0].Name != "edge-1" || tr.Clusters[1].Name != "prod" {
		t.Fatalf("tree: %+v", tr)
	}
	prod := tr.Clusters[1]
	if len(prod.Namespaces) != 2 || prod.Namespaces[0].Name != "db" || prod.Namespaces[1].Name != "payments" {
		t.Fatalf("namespaces not sorted: %+v", prod.Namespaces)
	}
	api1 := prod.Namespaces[1].Pods[0]
	if api1.Name != "api-1" || api1.UID != "uid-1" || api1.Node != "n1" || !api1.Live || len(api1.Containers) != 2 || api1.Containers[0].Name != "app" || api1.Containers[1].Name != "sidecar" {
		t.Fatalf("pod api-1: %+v", api1)
	}
	if api1.Labels["app"] != "api" || api1.Labels["team"] != "pay" {
		t.Fatalf("labels: %+v", api1.Labels)
	}
	if pg := prod.Namespaces[0].Pods[0]; pg.Restarts != 2 || pg.LastTS != base+300*1e9 {
		t.Fatalf("postgres: %+v", pg)
	}
	cases := map[string]int{
		"cluster=prod&namespace=payments":       2,
		"namespace=payments":                    3,
		"pod=api-*":                             3,
		"container=sidecar":                     1,
		"uid=uid-9":                             1,
		"label=team:pay":                        1,
		"label=app=api":                         1,
		"label=app:nope":                        0,
		"cluster=nowhere":                       0,
		"sid=" + ids["prod/payments/api-1/app"]: 1,
		"sid=" + ids["prod/payments/api-1/app"] + "," + ids["prod/db/postgres-0/db"]: 2,
		"sid=notanumber": 0,
	}
	for qs, want := range cases {
		if got := h.streams(t, qs, nil); got.Count != want {
			t.Errorf("streams?%s: count %d want %d", qs, got.Count, want)
		}
	}

	res := h.query(t, "", nil)
	if len(res.Lines) != 5 || res.Truncated {
		t.Fatalf("query all: %+v", res)
	}
	for i := 1; i < len(res.Lines); i++ {
		if res.Lines[i-1].T < res.Lines[i].T {
			t.Fatalf("not newest first: %+v", msgs(res.Lines))
		}
	}
	if res.Lines[0].M != "edge line" || res.Lines[4].M != "hello world" || res.Next != res.Lines[4].T-1 {
		t.Fatalf("order/next: %+v next=%d", msgs(res.Lines), res.Next)
	}
	if res = h.query(t, "q=boom", nil); len(res.Lines) != 1 || res.Lines[0].M != "error: boom" || res.Lines[0].SID != ids["prod/payments/api-1/app"] || res.Lines[0].E {
		t.Fatalf("filter: %+v", res)
	}
	if res = h.query(t, "limit=1", nil); len(res.Lines) != 1 || res.Lines[0].M != "edge line" || res.Next != res.Lines[0].T-1 {
		t.Fatalf("limit=1: %+v", res)
	}
	// paging: pass next as end to get the older page
	var page apiResult
	h.getJSON(t, "/api/v1/query?start=0&limit=2&end="+strconv.FormatInt(res.Next, 10), nil, &page)
	if len(page.Lines) != 2 || page.Lines[0].M != "ready to accept connections" || page.Lines[1].M != "proxy ok" {
		t.Fatalf("page 2: %+v", msgs(page.Lines))
	}
	for _, l := range []string{"0", "-5", "99999", "abc"} { // out of range → default 500
		if res = h.query(t, "limit="+l, nil); len(res.Lines) != 5 {
			t.Fatalf("limit=%s: %d lines", l, len(res.Lines))
		}
	}
	if res = h.query(t, "cluster=edge-1", nil); len(res.Lines) != 1 || res.Lines[0].M != "edge line" {
		t.Fatalf("cluster: %+v", res)
	}
	if res = h.query(t, "sid="+ids["prod/db/postgres-0/db"], nil); len(res.Lines) != 1 || res.Lines[0].R != 2 {
		t.Fatalf("sid + restart: %+v", res)
	}
	if res = h.query(t, "namespace=payments&q=proxy", nil); len(res.Lines) != 1 || res.Lines[0].M != "proxy ok" {
		t.Fatalf("ns+filter: %+v", res)
	}
	// time window: only the two app lines sit in [base, base+1s]
	var win apiResult
	h.getJSON(t, "/api/v1/query?start="+strconv.FormatInt(base, 10)+"&end="+strconv.FormatInt(base+1e9, 10), nil, &win)
	if len(win.Lines) != 2 {
		t.Fatalf("window: %+v", msgs(win.Lines))
	}
	// default window is the last hour: the 2023 fixture is outside it
	var def apiResult
	h.getJSON(t, "/api/v1/query", nil, &def)
	if len(def.Lines) != 0 {
		t.Fatalf("default window: %+v", msgs(def.Lines))
	}

	code, hdr, txt := h.getText(t, "/api/v1/export?start=0&end="+endNS, nil)
	lines := strings.Split(strings.TrimRight(txt, "\n"), "\n")
	if code != 200 || len(lines) != 5 {
		t.Fatalf("export: %d %q", code, txt)
	}
	if cd := hdr.Get("Content-Disposition"); !strings.HasPrefix(cd, `attachment; filename="p10logs-`) || !strings.HasSuffix(cd, `.txt"`) {
		t.Fatalf("disposition: %q", cd)
	}
	if !strings.HasSuffix(lines[0], "prod/payments/api-1/app hello world") || !strings.HasSuffix(lines[4], "edge-1/payments/api-2/app edge line") {
		t.Fatalf("export order (oldest first): %q", lines)
	}
	if !strings.HasPrefix(lines[0], "2023-11-14T22:13:20Z ") {
		t.Fatalf("export timestamp: %q", lines[0])
	}
	code, hdr, txt = h.getText(t, "/api/v1/export?start=0&end="+endNS+"&format=ndjson&q=boom", nil)
	var el exportLine
	if err := json.Unmarshal([]byte(strings.TrimSpace(txt)), &el); code != 200 || err != nil || el.M != "error: boom" || el.Stream != "prod/payments/api-1/app" || el.T != "2023-11-14T22:13:21Z" {
		t.Fatalf("ndjson export: %d %q %v", code, txt, err)
	}
	if !strings.HasSuffix(hdr.Get("Content-Disposition"), `.ndjson"`) {
		t.Fatalf("ndjson disposition: %q", hdr.Get("Content-Disposition"))
	}
	if _, _, txt = h.getText(t, "/api/v1/export?start=0&end="+endNS+"&sid="+ids["prod/db/postgres-0/db"], nil); strings.Count(txt, "\n") != 1 || !strings.Contains(txt, "ready to accept") {
		t.Fatalf("export sid: %q", txt)
	}
}

func TestMetricsAndUI(t *testing.T) {
	h := newHub(t, openAuth, Config{MetricsEnabled: true})
	h.mustPush(t, "prod", batch("ns/pod/uid/c", "f", 0, 10, 0, nil, "a", "b"))
	code, hdr, txt := h.getText(t, "/metrics", nil)
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/plain") || !strings.Contains(txt, "p10logs_ingest_lines_total 2\n") || !strings.Contains(txt, "p10logs_streams 1\n") || !strings.Contains(txt, "p10logs_tail_clients 0\n") {
		t.Fatalf("metrics: %d %q", code, txt)
	}
	for _, p := range []string{"/", "/index.html"} {
		if code, hdr, txt = h.getText(t, p, nil); code != 200 || !strings.Contains(txt, "p10logs ui") || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html") || hdr.Get("Cache-Control") != "no-cache" {
			t.Fatalf("ui %s: %d %q", p, code, txt)
		}
	}
	if code, _, _ = h.getText(t, "/nothing-here", nil); code != 404 {
		t.Fatalf("unknown path: %d", code)
	}

	off := newHub(t, openAuth, Config{})
	if code, _, _ = off.getText(t, "/metrics", nil); code != 404 {
		t.Fatalf("metrics disabled: %d", code)
	}
	dev := newHub(t, openAuth, Config{UIHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "dev "+r.URL.Path) })})
	if code, _, txt = dev.getText(t, "/some/asset.js", nil); code != 200 || txt != "dev /some/asset.js" {
		t.Fatalf("dev ui handler: %d %q", code, txt)
	}
}

// ---- auth and roles ----

func TestUIAuthAndRoles(t *testing.T) {
	h := newHub(t, auth.Config{
		Mode: "basic", BasicUser: "u", BasicPass: "p",
		IngestTokens: []string{"tok"}, APITokens: []string{"admintok"},
		Roles: []auth.Role{
			{Name: "pay", Users: []string{"U"}, APITokens: []string{"paytok"}, Namespaces: []string{"payments"}},
			{Name: "edge", APITokens: []string{"edgetok"}, Clusters: []string{"edge-*"}},
		},
	}, Config{})
	ids := seed(t, h, bearer("admintok"))

	code, hdr, _ := h.getText(t, "/api/v1/status", nil)
	if code != 401 || !strings.HasPrefix(hdr.Get("WWW-Authenticate"), "Basic") {
		t.Fatalf("anonymous api: %d %q", code, hdr.Get("WWW-Authenticate"))
	}
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/?sid=1", nil)
	resp, err := (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 302 || resp.Header.Get("Location") != "/auth/login?next="+url.QueryEscape("/?sid=1") {
		t.Fatalf("anonymous page: %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if code, _, _ = h.getText(t, "/api/v1/status", basic("u", "wrong")); code != 401 {
		t.Fatalf("wrong password: %d", code)
	}
	if code, _, _ = h.getText(t, "/api/v1/status", basic("nobody", "p")); code != 401 {
		t.Fatalf("unknown user: %d", code)
	}
	if code, _, _ = h.getText(t, "/api/v1/status", bearer("unknown")); code != 401 {
		t.Fatalf("unknown bearer: %d", code)
	}
	var me map[string]any
	if c := h.getJSON(t, "/auth/me", basic("u", "p"), &me); c != 200 || me["user"] != "u" || me["mode"] != "basic" {
		t.Fatalf("/auth/me: %d %+v", c, me)
	}
	if code, _, txt := h.getText(t, "/", basic("u", "p")); code != 200 || !strings.Contains(txt, "p10logs ui") {
		t.Fatalf("ui with creds: %d", code)
	}

	// user u is in role pay (case-insensitive match): payments only, both clusters
	tr := h.streams(t, "", basic("u", "p"))
	if tr.Count != 3 || len(tr.ids()) != 3 {
		t.Fatalf("role via user: %+v", tr.ids())
	}
	for k := range tr.ids() {
		if strings.Contains(k, "/db/") {
			t.Fatalf("role leaks db namespace: %+v", tr.ids())
		}
	}
	res := h.query(t, "", basic("u", "p"))
	if len(res.Lines) != 4 {
		t.Fatalf("role query via user: %+v", msgs(res.Lines))
	}
	// asking for the forbidden stream by sid is also denied
	if res = h.query(t, "sid="+ids["prod/db/postgres-0/db"], basic("u", "p")); len(res.Lines) != 0 {
		t.Fatalf("sid outside role: %+v", msgs(res.Lines))
	}
	// same role through its API token
	if res = h.query(t, "namespace=db", bearer("paytok")); len(res.Lines) != 0 {
		t.Fatalf("paytok db: %+v", msgs(res.Lines))
	}
	if res = h.query(t, "", bearer("paytok")); len(res.Lines) != 4 {
		t.Fatalf("paytok: %+v", msgs(res.Lines))
	}
	// cluster glob role
	if res = h.query(t, "", bearer("edgetok")); len(res.Lines) != 1 || res.Lines[0].M != "edge line" {
		t.Fatalf("edgetok: %+v", msgs(res.Lines))
	}
	if tr = h.streams(t, "", bearer("edgetok")); tr.Count != 1 || tr.Clusters[0].Name != "edge-1" {
		t.Fatalf("edgetok streams: %+v", tr.ids())
	}
	// global API token is unrestricted
	if res = h.query(t, "", bearer("admintok")); len(res.Lines) != 5 {
		t.Fatalf("admintok: %+v", msgs(res.Lines))
	}
	_, _, txt := h.getText(t, "/api/v1/export?start=0&end="+endNS, bearer("paytok"))
	if strings.Count(txt, "\n") != 4 || strings.Contains(txt, "postgres") {
		t.Fatalf("export scoped: %q", txt)
	}

	// roles configured but the identity matches none → sees nothing
	deny := newHub(t, auth.Config{Mode: "basic", BasicUser: "u", BasicPass: "p", IngestTokens: []string{"tok"}, APITokens: []string{"tok2"},
		Roles: []auth.Role{{Name: "x", Users: []string{"someone-else"}}}}, Config{})
	seed(t, deny, bearer("tok2"))
	if tr = deny.streams(t, "", basic("u", "p")); tr.Count != 0 || len(tr.Clusters) != 0 {
		t.Fatalf("deny-all streams: %+v", tr)
	}
	if res = deny.query(t, "", basic("u", "p")); len(res.Lines) != 0 {
		t.Fatalf("deny-all query: %+v", msgs(res.Lines))
	}
}

func TestHopsGuard(t *testing.T) {
	h := newHub(t, openAuth, Config{})
	for _, p := range []string{"/api/v1/streams", "/api/v1/query", "/api/v1/export", "/api/v1/status", "/api/v1/tail"} {
		var e map[string]string
		if c := h.getJSON(t, p, map[string]string{wire.HdrHops: "9"}, nil); c != 508 {
			t.Fatalf("%s with hops=9: %d", p, c)
		}
		resp := h.do(t, http.MethodGet, p, "", map[string]string{wire.HdrHops: "4"})
		json.NewDecoder(resp.Body).Decode(&e)
		resp.Body.Close()
		if resp.StatusCode != 508 || e["error"] != "federation loop" {
			t.Fatalf("%s with hops=4: %d %+v", p, resp.StatusCode, e)
		}
	}
	if c := h.getJSON(t, "/api/v1/status", map[string]string{wire.HdrHops: "3"}, nil); c != 200 {
		t.Fatalf("hops=3 is the last allowed: %d", c)
	}
}

func TestQueryBusyAndNoFlusher(t *testing.T) {
	h := newHub(t, openAuth, Config{MaxConcurrent: 1})
	h.s.sem <- struct{}{} // one query "in flight"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, fn := range []http.HandlerFunc{h.s.query, h.s.export} {
		rec := httptest.NewRecorder()
		fn(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query", nil).WithContext(ctx))
		var e map[string]string
		json.Unmarshal(rec.Body.Bytes(), &e)
		if rec.Code != 503 || e["error"] != "too many concurrent queries" {
			t.Fatalf("busy: %d %s", rec.Code, rec.Body)
		}
	}
	<-h.s.sem
	rec := httptest.NewRecorder()
	h.s.query(rec, httptest.NewRequest(http.MethodGet, "/api/v1/query?start=0&end="+endNS, nil))
	if rec.Code != 200 {
		t.Fatalf("after release: %d %s", rec.Code, rec.Body)
	}
	// tail needs a flusher
	nf := struct{ http.ResponseWriter }{httptest.NewRecorder()}
	rec2 := nf.ResponseWriter.(*httptest.ResponseRecorder)
	h.s.tail(nf, httptest.NewRequest(http.MethodGet, "/api/v1/tail", nil))
	if rec2.Code != 500 || !strings.Contains(rec2.Body.String(), "streaming unsupported") {
		t.Fatalf("no flusher: %d %s", rec2.Code, rec2.Body)
	}
}

// ---- tail ----

type sse struct {
	resp   *http.Response
	rd     *bufio.Reader
	cancel context.CancelFunc
}

func openTail(t *testing.T, h *hub, qs string) *sse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.srv.URL+"/api/v1/tail?"+qs, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	s := &sse{resp: resp, rd: bufio.NewReader(resp.Body), cancel: cancel}
	t.Cleanup(s.close)
	if resp.StatusCode != 200 {
		return s
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("tail content-type: %q", ct)
	}
	if l := s.line(t); l != ": p10logs tail" {
		t.Fatalf("tail preamble: %q", l)
	}
	return s
}

func (s *sse) close() { s.cancel(); s.resp.Body.Close() }

func (s *sse) line(t *testing.T) string {
	t.Helper()
	for {
		l, err := s.rd.ReadString('\n')
		if err != nil {
			t.Fatalf("tail read: %v", err)
		}
		if l = strings.TrimRight(l, "\n"); l != "" {
			return l
		}
	}
}

// data waits for the next data: event and decodes it.
func (s *sse) data(t *testing.T) map[string]any {
	t.Helper()
	for {
		l := s.line(t)
		if !strings.HasPrefix(l, "data: ") {
			continue
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(l[6:]), &v); err != nil {
			t.Fatalf("tail event %q: %v", l, err)
		}
		return v
	}
}

func TestTailSSE(t *testing.T) {
	h := newHub(t, openAuth, Config{TailMaxClients: 1})
	ids := seed(t, h, nil)
	tl := openTail(t, h, "namespace=payments&q=want")
	var st struct {
		Hub map[string]any `json:"hub"`
	}
	h.getJSON(t, "/api/v1/status", nil, &st)
	if st.Hub["tail_clients"].(float64) != 1 {
		t.Fatalf("tail_clients: %v", st.Hub["tail_clients"])
	}
	if c := h.getJSON(t, "/api/v1/tail", nil, nil); c != 503 {
		t.Fatalf("second client over maxClients: %d", c)
	}
	// db is outside the selector, "skip this" fails the filter; only "want this" arrives
	h.mustPush(t, "prod", batch("db/postgres-0/uid-9/db", "f3", 310, 320, 2, nil, "want this too but wrong namespace"))
	h.mustPush(t, "prod", batch("payments/api-1/uid-1/app", "f1", 100, 200, 0, nil, "skip this", "want this"))
	ev := tl.data(t)
	if ev["m"] != "want this" || ev["sid"] != ids["prod/payments/api-1/app"] || ev["t"].(float64) != float64(base+101*1e9) {
		t.Fatalf("tail event: %+v", ev)
	}
	tl.close()
	deadline := time.Now().Add(3 * time.Second)
	for h.st.Bus.Count() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if h.st.Bus.Count() != 0 {
		t.Fatal("tail did not unsubscribe after the client left")
	}
	// sid selector and stderr flag
	tl = openTail(t, h, "sid="+ids["prod/db/postgres-0/db"])
	h.mustPush(t, "prod", batch("payments/api-1/uid-1/app", "f1", 200, 300, 0, nil, "not for this tail"))
	h.mustPush(t, "prod", `{"k":"db/postgres-0/uid-9/db","f":"f3","o":[320,330],"r":2}`+"\n"+`{"t":`+strconv.FormatInt(base+320*1e9, 10)+`,"s":"e","m":"stderr line"}`+"\n")
	if ev = tl.data(t); ev["m"] != "stderr line" || ev["e"] != true || ev["r"].(float64) != 2 {
		t.Fatalf("tail by sid: %+v", ev)
	}
}

func TestTailDropped(t *testing.T) {
	h := newHub(t, openAuth, Config{TailBuffer: 2, TailLinesPerSec: 10000})
	h.mustPush(t, "prod", batch("ns/pod/uid/c", "f", 0, 1, 0, nil, "warm up"))
	tl := openTail(t, h, "namespace=ns")
	lines := make([]string, 200)
	for i := range lines {
		lines[i] = "burst " + strconv.Itoa(i)
	}
	h.mustPush(t, "prod", batch("ns/pod/uid/c", "f", 1, 300, 0, nil, lines...))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ev := tl.data(t); ev["dropped"] != nil {
			if ev["dropped"].(float64) <= 0 {
				t.Fatalf("dropped: %+v", ev)
			}
			return
		}
	}
	t.Fatal("no dropped marker for a slow tail client")
}

// ---- federation ----

func TestFederation(t *testing.T) {
	a := newHub(t, openAuth, Config{})
	ids := seed(t, a, nil)
	appID := ids["prod/payments/api-1/app"]
	b := newHub(t, openAuth, Config{Peers: federation.New([]federation.Peer{{Name: "a", URL: a.srv.URL + "/", Token: "fedtok"}})})
	b.mustPush(t, "local", batch("ops/web-1/uid-5/web", "f", 900, 910, 0, nil, "local line"))

	tr := b.streams(t, "", nil)
	if tr.Count != 5 || len(tr.Clusters) != 3 {
		t.Fatalf("federated tree: %+v", tr)
	}
	// sorted by hub+name: peer clusters ("a…") sort before the local one ("" + "local")
	if tr.Clusters[0].Hub != "a" || tr.Clusters[0].Name != "edge-1" || tr.Clusters[1].Hub != "a" || tr.Clusters[1].Name != "prod" || tr.Clusters[2].Hub != "" || tr.Clusters[2].Name != "local" {
		t.Fatalf("hub tags/order: %+v %+v %+v", tr.Clusters[0], tr.Clusters[1], tr.Clusters[2])
	}
	if got := tr.ids()["prod/payments/api-1/app"]; got != "a/"+appID {
		t.Fatalf("peer sid prefix: %q", got)
	}
	localID := tr.ids()["local/ops/web-1/web"]

	res := b.query(t, "", nil)
	if len(res.Lines) != 6 || res.Lines[0].M != "local line" || res.Lines[0].SID != localID || res.Lines[1].SID != "a/"+ids["edge-1/payments/api-2/app"] || res.Chunks < 0 {
		t.Fatalf("federated query: %+v", res)
	}
	if res = b.query(t, "sid=a/"+appID, nil); len(res.Lines) != 2 || res.Lines[0].SID != "a/"+appID {
		t.Fatalf("query by peer sid: %+v", res)
	}
	if res = b.query(t, "sid="+localID, nil); len(res.Lines) != 1 || res.Lines[0].M != "local line" {
		t.Fatalf("query by local sid: %+v", res)
	}
	if res = b.query(t, "q=boom&limit=1", nil); len(res.Lines) != 1 || res.Lines[0].M != "error: boom" || res.Next != res.Lines[0].T-1 {
		t.Fatalf("federated filter: %+v", res)
	}
	if res = b.query(t, "limit=2", nil); len(res.Lines) != 2 || !res.Truncated {
		t.Fatalf("federated limit: %+v", res)
	}
	// streams limited to peer sids skip the local tree
	if tr = b.streams(t, "sid=a/"+appID, nil); tr.Count != 1 || tr.Clusters[0].Hub != "a" {
		t.Fatalf("streams by peer sid: %+v", tr)
	}

	_, _, txt := b.getText(t, "/api/v1/export?start=0&end="+endNS, nil)
	lines := strings.Split(strings.TrimRight(txt, "\n"), "\n")
	if len(lines) != 6 || !strings.Contains(lines[0], " a:prod/payments/api-1/app hello world") || !strings.HasSuffix(lines[5], " local/ops/web-1/web local line") {
		t.Fatalf("federated export: %q", lines)
	}
	_, _, txt = b.getText(t, "/api/v1/export?start=0&end="+endNS+"&format=ndjson&sid=a/"+appID, nil)
	if strings.Count(txt, "\n") != 2 || !strings.Contains(txt, `"stream":"a:prod/payments/api-1/app"`) {
		t.Fatalf("federated ndjson export: %q", txt)
	}

	var st struct {
		Hub   map[string]any   `json:"hub"`
		Peers []map[string]any `json:"peers"`
	}
	b.getJSON(t, "/api/v1/status", nil, &st)
	if len(st.Peers) != 1 || st.Peers[0]["name"] != "a" || st.Peers[0]["ok"] != true || st.Peers[0]["hub"] == nil || st.Peers[0]["url"] != a.srv.URL+"/" {
		t.Fatalf("federated status: %+v", st.Peers)
	}

	// hop budget: a request that already used all hops is served locally, the peer fails
	if tr = b.streams(t, "", map[string]string{wire.HdrHops: "3"}); tr.Count != 1 || len(tr.PeersFailed) != 1 || tr.PeersFailed[0] != "a" {
		t.Fatalf("hop budget: %+v", tr)
	}

	// live tail through the peer
	tl := openTail(t, b, "namespace=payments")
	a.mustPush(t, "prod", batch("payments/api-1/uid-1/app", "f1", 100, 200, 0, nil, "relayed"))
	if ev := tl.data(t); ev["m"] != "relayed" || ev["sid"] != "a/"+appID {
		t.Fatalf("federated tail: %+v", ev)
	}
	tl.close()

	// a peer that is down degrades, it does not fail the request
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	c := newHub(t, openAuth, Config{Peers: federation.New([]federation.Peer{{Name: "dead", URL: dead.URL, Token: "x"}, {Name: "a", URL: a.srv.URL, Token: "fedtok"}})})
	if tr = c.streams(t, "", nil); tr.Count != 4 || len(tr.PeersFailed) != 1 || tr.PeersFailed[0] != "dead" {
		t.Fatalf("dead peer streams: %+v", tr)
	}
	if res = c.query(t, "", nil); len(res.Lines) != 6 || len(res.PeersFailed) != 1 || res.PeersFailed[0] != "dead" { // a has 6 lines after "relayed"
		t.Fatalf("dead peer query: %+v", res)
	}
	c.getJSON(t, "/api/v1/status", nil, &st)
	if len(st.Peers) != 2 || st.Peers[0]["name"] != "a" || st.Peers[0]["ok"] != true || st.Peers[1]["name"] != "dead" || st.Peers[1]["ok"] != false || st.Peers[1]["error"] == nil {
		t.Fatalf("dead peer status: %+v", st.Peers)
	}
	if _, _, txt = c.getText(t, "/api/v1/export?start=0&end="+endNS, nil); strings.Count(txt, "\n") != 6 {
		t.Fatalf("dead peer export: %q", txt)
	}
}

// ---- helpers ----

func TestSelectorFromAndPeerQuery(t *testing.T) {
	q := url.Values{"cluster": {"c"}, "namespace": {"n"}, "pod": {"p"}, "container": {"k"}, "uid": {"u"}, "since": {"1700000000"}, "label": {"app:api", "team=pay", "novalue", ":x"}}
	sel := selectorFrom(q, []string{"7", "x", "9"})
	if sel.Cluster != "c" || sel.Namespace != "n" || sel.Pod != "p" || sel.Container != "k" || sel.UID != "u" || sel.Since != 1700000000*1e9 {
		t.Fatalf("selector: %+v", sel)
	}
	if len(sel.IDs) != 2 || sel.IDs[0] != 7 || sel.IDs[1] != 9 {
		t.Fatalf("ids: %+v", sel.IDs)
	}
	if len(sel.Labels) != 2 || sel.Labels["app"] != "api" || sel.Labels["team"] != "pay" {
		t.Fatalf("labels: %+v", sel.Labels)
	}
	if sel = selectorFrom(url.Values{}, []string{"x"}); len(sel.IDs) != 1 || sel.IDs[0] != -1 {
		t.Fatalf("unparseable sids must match nothing: %+v", sel.IDs)
	}
	if sel = selectorFrom(url.Values{}, nil); sel.IDs != nil || sel.Labels != nil || sel.Since != 0 {
		t.Fatalf("empty: %+v", sel)
	}
	pq := peerQuery(url.Values{"sid": {"a/1,b/2"}, "q": {"x"}, "limit": {"5"}}, []string{"1", "3"})
	if pq.Get("sid") != "1,3" || pq.Get("q") != "x" || pq.Get("limit") != "5" {
		t.Fatalf("peerQuery: %+v", pq)
	}
	if pq = peerQuery(url.Values{"sid": {"a/1"}}, nil); pq.Has("sid") {
		t.Fatalf("peerQuery without sids: %+v", pq)
	}
}

func TestParseRelativeAndTime(t *testing.T) {
	for in, want := range map[string]time.Duration{"15m": 15 * time.Minute, "1.5h": 90 * time.Minute, "2d": 48 * time.Hour, "0.5d": 12 * time.Hour, "1w": 7 * 24 * time.Hour, "0s": 0} {
		if d, ok := parseRelative(in); !ok || d != want {
			t.Errorf("parseRelative(%q) = %v %v", in, d, ok)
		}
	}
	for _, in := range []string{"", "d", "xd", "-1d", "-5m", "abc", "1y"} {
		if _, ok := parseRelative(in); ok {
			t.Errorf("parseRelative(%q) should fail", in)
		}
	}
	def := int64(42)
	if parseTime("", def) != def || parseTime("garbage", def) != def || parseTime("-1y", def) != def || parseTime(" 1700000000 ", def) != 1700000000*1e9 {
		t.Fatal("parseTime defaults")
	}
	if parseTime("2023-11-14T22:13:20Z", def) != base || parseTime("2023-11-14T22:13:20.5Z", def) != base+5e8 {
		t.Fatal("parseTime rfc3339")
	}
	if parseTime("1700000000000", def) != 1700000000*1e9 || parseTime("1700000000000000", def) != 1700000000*1e9 || parseTime("1700000000000000000", def) != 1700000000*1e9 {
		t.Fatal("parseTime millis/micros/nanos")
	}
	if now := time.Now().UnixNano(); parseTime("now", def)-now > int64(time.Second) || parseTime("-0s", def)-now > int64(time.Second) {
		t.Fatal("parseTime now")
	}
}

func TestToAPIAndFirst(t *testing.T) {
	out := toAPI(store.Result{Lines: []store.Line{{SID: 3, TS: 9, Stderr: true, Msg: "m", Restart: 1}}, Chunks: 2, Scanned: 10, Millis: 1, Truncated: true, Skipped: 4, Next: 8}, "p/")
	if len(out.Lines) != 1 || out.Lines[0].SID != "p/3" || out.Lines[0].T != 9 || !out.Lines[0].E || out.Lines[0].M != "m" || out.Lines[0].R != 1 {
		t.Fatalf("toAPI lines: %+v", out.Lines)
	}
	if out.Chunks != 2 || out.Scanned != 10 || out.Millis != 1 || !out.Truncated || out.Skipped != 4 || out.Next != 8 {
		t.Fatalf("toAPI stats: %+v", out)
	}
	if first(nil) != "" || first([]string{"a", "b"}) != "a" {
		t.Fatal("first")
	}
	s := New(Config{}, nil, nil, nil)
	if cap(s.sem) != 8 {
		t.Fatalf("default MaxConcurrent: %d", cap(s.sem))
	}
	if !s.allow("any", 1<<40) {
		t.Fatal("no PerClusterBPS means no limit")
	}
}
