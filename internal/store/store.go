// Package store is the hub's storage engine.
//
// Write path: push → WAL append + fsync → cursor → memtable (per stream) → ack.
// Memtables are flushed to per-stream chunks as large zstd frames (≥ flushBytes or
// ≥ flushAge), chunks live under UTC day directories and are sealed at targetBytes,
// and the SQLite catalog is derived state that can be rebuilt from the files.
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/p10node/p10logs/internal/bloom"
	"github.com/p10node/p10logs/internal/chunk"
	"github.com/p10node/p10logs/internal/index"
	"github.com/p10node/p10logs/internal/query"
	"github.com/p10node/p10logs/internal/s3"
	"github.com/p10node/p10logs/internal/tailbus"
	"github.com/p10node/p10logs/internal/wal"
	"github.com/p10node/p10logs/internal/wire"
)

// Override is a per-namespace retention rule; Match globs "<cluster>/<namespace>".
type Override struct {
	Match  string
	MaxAge time.Duration
}

// Config for the store.
type Config struct {
	Dir             string
	TargetBytes     int64         // seal chunk at this size
	MaxOpenAge      time.Duration // seal chunk when idle this long
	FlushBytes      int64         // flush a stream's memtable to a frame at this size
	FlushAge        time.Duration // ... or when its oldest entry is this old
	MemMaxBytes     int64         // total memtable budget; largest streams flush first
	WALSegmentBytes int64         // checkpoint (flush all + truncate WAL) at this size
	MaxAge          time.Duration
	MaxDiskBytes    int64
	Overrides       []Override
	MaxScanBytes    int64
	QueryTimeout    time.Duration
	MaxLineBytes    int
	// Object storage (optional): sealed chunks older than UploadAfter are copied to
	// the bucket; local copies are evicted first when the disk cap is hit; queries
	// fetch cold chunks into a bounded local cache.
	Object       *s3.Client
	UploadAfter  time.Duration
	OffloadEvery time.Duration
	CacheBytes   int64
}

type openChunk struct {
	w        *chunk.Writer
	path     string
	day      string
	streamID int64
	cluster  string
	key      string
	created  time.Time
	last     time.Time
}

// memStream buffers accepted lines of one stream until they are flushed as a frame.
type memStream struct {
	entries []chunk.Entry // owned copies
	bytes   int64
	first   time.Time
	meta    chunk.Meta // last section's meta (cursor, labels, restart, node)
	minTS   int64
	maxTS   int64
	wseq    uint64 // max WAL seq buffered
}

// AgentInfo is what the hub knows about one agent from its pushes.
type AgentInfo struct {
	Cluster    string `json:"cluster"`
	Node       string `json:"node"`
	Version    string `json:"version"`
	LastSeen   int64  `json:"last_seen"`
	Files      int64  `json:"files"`
	LagBytes   int64  `json:"lag_bytes"`
	SpoolBytes int64  `json:"spool_bytes"`
	Dropped    int64  `json:"dropped"`
	RSS        int64  `json:"rss,omitempty"`
}

// Store is the engine.
type Store struct {
	cfg  Config
	Idx  *index.DB
	Bus  *tailbus.Bus
	Log  *slog.Logger
	mu   sync.Mutex
	wal  *wal.Log
	open map[int64]*openChunk
	mem  map[int64]*memStream
	memB int64
	curs map[string]int64
	wseq map[int64]uint64 // flushed WAL seq per stream

	amu    sync.Mutex
	agents map[string]*AgentInfo

	InBytes   atomic.Int64
	InLines   atomic.Int64
	RateBytes atomic.Int64 // per second, sampled
	RateLines atomic.Int64
	Deduped   atomic.Int64
	started   time.Time
}

// Open initialises the store: recovers open chunks, then replays the WAL.
func Open(cfg Config, log *slog.Logger) (*Store, error) {
	if cfg.TargetBytes == 0 {
		cfg.TargetBytes = 4 << 20
	}
	if cfg.MaxOpenAge == 0 {
		cfg.MaxOpenAge = 5 * time.Minute
	}
	if cfg.FlushBytes == 0 {
		cfg.FlushBytes = 256 << 10
	}
	if cfg.FlushAge == 0 {
		cfg.FlushAge = 30 * time.Second
	}
	if cfg.MemMaxBytes == 0 {
		cfg.MemMaxBytes = 128 << 20
	}
	if cfg.WALSegmentBytes == 0 {
		cfg.WALSegmentBytes = 64 << 20
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 168 * time.Hour
	}
	if cfg.MaxScanBytes == 0 {
		cfg.MaxScanBytes = 2 << 30
	}
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 60 * time.Second
	}
	if cfg.MaxLineBytes == 0 {
		cfg.MaxLineBytes = 256 << 10
	}
	if cfg.UploadAfter == 0 {
		cfg.UploadAfter = time.Hour
	}
	if cfg.CacheBytes == 0 {
		cfg.CacheBytes = 1 << 30
	}
	if cfg.OffloadEvery == 0 {
		cfg.OffloadEvery = time.Minute
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, "days"), 0o755); err != nil {
		return nil, err
	}
	idx, err := index.Open(filepath.Join(cfg.Dir, "index.sqlite"))
	if err != nil {
		return nil, err
	}
	s := &Store{cfg: cfg, Idx: idx, Bus: tailbus.New(), Log: log, open: map[int64]*openChunk{}, mem: map[int64]*memStream{},
		curs: map[string]int64{}, wseq: map[int64]uint64{}, agents: map[string]*AgentInfo{}, started: time.Now()}
	if err := s.recoverOpen(); err != nil {
		return nil, err
	}
	if s.wseq, err = idx.WSeqs(); err != nil {
		return nil, err
	}
	for sid, oc := range s.open { // open chunks know their own high-water mark
		if oc.w.WSeq > s.wseq[sid] {
			s.wseq[sid] = oc.w.WSeq
		}
	}
	replayed := 0
	s.wal, err = wal.Open(filepath.Join(cfg.Dir, "wal"), func(r wal.Record) error {
		if s.replay(r) {
			replayed++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if replayed > 0 {
		s.Log.Info("wal replayed", "records", replayed, "memtable_bytes", s.memB)
	}
	return s, nil
}

// Close flushes memtables to chunks, checkpoints the WAL and closes files.
func (s *Store) Close() error {
	s.mu.Lock()
	s.flushAll()
	s.wal.Checkpoint()
	s.wal.Close()
	for _, oc := range s.open {
		oc.w.Close()
	}
	s.open = map[int64]*openChunk{}
	s.mu.Unlock()
	return s.Idx.Close()
}

func splitKey(key string) (ns, pod, uid, ctr string, ok bool) {
	p := strings.SplitN(key, "/", 4)
	if len(p) != 4 {
		return "", "", "", "", false
	}
	return p[0], p[1], p[2], p[3], true
}

func (s *Store) streamFor(cluster, key, node string, restart int, minTS, maxTS int64, ended bool, labels map[string]string) (int64, error) {
	ns, pod, uid, ctr, ok := splitKey(key)
	if !ok {
		return 0, fmt.Errorf("bad stream key %q", key)
	}
	return s.Idx.UpsertStream(index.Stream{Cluster: cluster, Namespace: ns, Pod: pod, UID: uid, Container: ctr, Node: node, Restarts: restart, FirstTS: minTS, LastTS: maxTS, Ended: ended, Labels: labels})
}

// recoverOpen scans *.open files, truncates corrupt tails and re-derives cursors.
func (s *Store) recoverOpen() error {
	root := filepath.Join(s.cfg.Dir, "days")
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".open") {
			return nil
		}
		w, err := chunk.Create(p)
		if err != nil {
			s.Log.Warn("recover: unreadable open chunk, removing", "path", p, "err", err)
			os.Remove(p)
			return nil
		}
		if len(w.Frames) == 0 || w.Key == "" {
			w.Close()
			os.Remove(p)
			return nil
		}
		lm := w.LastMeta()
		sid, err := s.streamFor(w.Cluster, w.Key, lm.Node, w.Restart, w.MinTS, w.MaxTS, lm.End, w.Labels)
		if err != nil {
			w.Close()
			return nil
		}
		oc := &openChunk{w: w, path: p, day: dayOf(p), streamID: sid, cluster: w.Cluster, key: w.Key, created: time.Now(), last: time.Now()}
		if prev, dup := s.open[sid]; dup { // two open chunks for one stream: seal the older one now
			s.seal(prev)
		}
		s.open[sid] = oc
		s.setCursor(sid, lm.File, lm.Off[1])
		s.Log.Info("recovered open chunk", "path", p, "frames", len(w.Frames), "lines", w.Lines)
		return nil
	})
}

// replay puts a WAL record back into its memtable unless it already reached a chunk.
func (s *Store) replay(r wal.Record) bool {
	m := r.Meta
	sid, err := s.streamFor(m.Cluster, m.Key, m.Node, m.Restart, m.MinTS, m.MaxTS, m.End, m.Labels)
	if err != nil {
		return false
	}
	s.setCursor(sid, m.File, m.Off[1])
	if r.Seq <= s.wseq[sid] || len(r.Entries) == 0 {
		return false
	}
	s.bufferLocked(sid, m, r.Entries, r.Seq)
	return true
}

func dayOf(p string) string {
	parts := strings.Split(filepath.ToSlash(p), "/")
	for i, x := range parts {
		if x == "days" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func (s *Store) cursor(sid int64, file string) int64 {
	k := strconv.FormatInt(sid, 10) + "|" + file
	if v, ok := s.curs[k]; ok {
		return v
	}
	v := s.Idx.GetCursor(sid, file)
	s.curs[k] = v
	return v
}

func (s *Store) setCursor(sid int64, file string, end int64) {
	k := strconv.FormatInt(sid, 10) + "|" + file
	if end > s.curs[k] {
		s.curs[k] = end
		s.Idx.SetCursor(sid, file, end)
	}
}

// Heartbeat records agent liveness and stats.
func (s *Store) Heartbeat(cluster, node, version, stats string) {
	s.amu.Lock()
	defer s.amu.Unlock()
	k := cluster + "|" + node
	a := s.agents[k]
	if a == nil {
		a = &AgentInfo{Cluster: cluster, Node: node}
		s.agents[k] = a
	}
	a.Version = version
	a.LastSeen = time.Now().UnixNano()
	if stats != "" {
		for _, kv := range strings.Split(stats, ";") {
			if i := strings.IndexByte(kv, '='); i > 0 {
				v, _ := strconv.ParseInt(kv[i+1:], 10, 64)
				switch kv[:i] {
				case "files":
					a.Files = v
				case "lag_bytes":
					a.LagBytes = v
				case "spool_bytes":
					a.SpoolBytes = v
				case "dropped":
					a.Dropped = v
				case "rss":
					a.RSS = v
				}
			}
		}
	}
}

// Agents lists known agents.
func (s *Store) Agents() []AgentInfo {
	s.amu.Lock()
	defer s.amu.Unlock()
	out := make([]AgentInfo, 0, len(s.agents))
	for _, a := range s.agents {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cluster != out[j].Cluster {
			return out[i].Cluster < out[j].Cluster
		}
		return out[i].Node < out[j].Node
	})
	return out
}

// IngestResult summarises one push.
type IngestResult struct {
	Lines, Deduped, Sections int
}

// Ingest parses an NDJSON push body (already decompressed) and stores it.
func (s *Store) Ingest(cluster, node string, r io.Reader) (IngestResult, error) {
	var res IngestResult
	br := bufio.NewReaderSize(r, 256<<10)
	var sec *wire.Section
	var ents []chunk.Entry
	commit := func() error {
		if sec == nil {
			return nil
		}
		res.Sections++
		n, d, err := s.commit(cluster, node, sec, ents)
		res.Lines += n
		res.Deduped += d
		sec, ents = nil, ents[:0]
		return err
	}
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 1 {
			if isSection(line) {
				if err := commit(); err != nil {
					return res, err
				}
				var sc wire.Section
				if err := json.Unmarshal(line, &sc); err != nil {
					return res, fmt.Errorf("bad section: %w", err)
				}
				sec = &sc
			} else if sec != nil {
				var e wire.Entry
				if err := json.Unmarshal(line, &e); err != nil {
					return res, fmt.Errorf("bad entry: %w", err)
				}
				if len(e.M) > s.cfg.MaxLineBytes {
					e.M = e.M[:s.cfg.MaxLineBytes] + "…[truncated]"
				}
				ents = append(ents, chunk.Entry{TS: e.T, Stderr: e.S == "e", Msg: []byte(e.M)})
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, err
		}
	}
	return res, commit()
}

func isSection(line []byte) bool {
	// section headers start with {"k": ; entries with {"t":
	return len(line) > 4 && line[1] == '"' && line[2] == 'k' && line[3] == '"'
}

// commit makes one section durable (WAL), then buffers it. Returns accepted and deduped counts.
func (s *Store) commit(cluster, node string, sec *wire.Section, ents []chunk.Entry) (int, int, error) {
	var minTS, maxTS int64
	for i, e := range ents {
		if i == 0 || e.TS < minTS {
			minTS = e.TS
		}
		if e.TS > maxTS {
			maxTS = e.TS
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sid, err := s.streamFor(cluster, sec.Key, node, sec.Restart, minTS, maxTS, sec.End, sec.Labels)
	if err != nil {
		return 0, 0, err
	}
	if len(ents) == 0 {
		return 0, 0, nil
	}
	if sec.Off[1] <= s.cursor(sid, sec.File) {
		s.Deduped.Add(int64(len(ents)))
		return 0, len(ents), nil // exact resend of an already-durable range
	}
	meta := chunk.Meta{Cluster: cluster, Key: sec.Key, File: sec.File, Off: sec.Off, Node: node, Restart: sec.Restart, End: sec.End, Labels: sec.Labels, MinTS: minTS, MaxTS: maxTS, Count: len(ents)}
	seq, err := s.wal.Append(meta, ents)
	if err != nil {
		return 0, 0, err
	}
	s.setCursor(sid, sec.File, sec.Off[1])
	s.bufferLocked(sid, meta, ents, seq)
	var raw int64
	evs := make([]tailbus.Event, len(ents))
	for i, e := range ents {
		raw += int64(len(e.Msg)) + 13
		evs[i] = tailbus.Event{StreamID: sid, TS: e.TS, Stderr: e.Stderr, Msg: string(e.Msg), Restart: sec.Restart}
	}
	s.InBytes.Add(raw)
	s.InLines.Add(int64(len(ents)))
	s.Bus.Publish(sid, evs)
	if m := s.mem[sid]; m != nil && m.bytes >= s.cfg.FlushBytes {
		s.flushStream(sid)
	}
	for s.memB > s.cfg.MemMaxBytes {
		s.flushLargest()
	}
	if s.wal.Size() >= s.cfg.WALSegmentBytes {
		s.checkpoint()
	}
	return len(ents), 0, nil
}

// bufferLocked appends owned copies of ents to the stream's memtable.
func (s *Store) bufferLocked(sid int64, meta chunk.Meta, ents []chunk.Entry, seq uint64) {
	m := s.mem[sid]
	if m == nil {
		m = &memStream{first: time.Now(), minTS: meta.MinTS, maxTS: meta.MaxTS}
		s.mem[sid] = m
	}
	for _, e := range ents {
		m.entries = append(m.entries, chunk.Entry{TS: e.TS, Stderr: e.Stderr, Msg: append([]byte(nil), e.Msg...)})
		m.bytes += int64(len(e.Msg)) + 13
		s.memB += int64(len(e.Msg)) + 13
	}
	m.meta = meta
	if meta.MinTS < m.minTS {
		m.minTS = meta.MinTS
	}
	if meta.MaxTS > m.maxTS {
		m.maxTS = meta.MaxTS
	}
	if seq > m.wseq {
		m.wseq = seq
	}
}

// flushStream writes a stream's memtable as one frame. Caller holds s.mu.
func (s *Store) flushStream(sid int64) {
	m := s.mem[sid]
	if m == nil || len(m.entries) == 0 {
		delete(s.mem, sid)
		return
	}
	oc := s.open[sid]
	if oc == nil {
		var err error
		if oc, err = s.newOpen(sid, m.meta.Cluster, m.meta.Key, m.minTS); err != nil {
			s.Log.Error("open chunk", "err", err)
			return
		}
	}
	meta := m.meta
	meta.MinTS, meta.MaxTS, meta.Count, meta.WSeq = m.minTS, m.maxTS, len(m.entries), m.wseq
	if _, err := oc.w.Append(meta, m.entries); err != nil {
		s.Log.Error("flush frame", "path", oc.path, "err", err)
		return
	}
	oc.last = time.Now()
	s.wseq[sid] = m.wseq
	s.Idx.SetWSeq(sid, m.wseq)
	s.memB -= m.bytes
	delete(s.mem, sid)
	if oc.w.Size() >= s.cfg.TargetBytes {
		s.seal(oc)
	}
}

func (s *Store) flushLargest() {
	var best int64
	var bestB int64 = -1
	for sid, m := range s.mem {
		if m.bytes > bestB {
			best, bestB = sid, m.bytes
		}
	}
	if bestB < 0 {
		return
	}
	s.flushStream(best)
}

// flushAll flushes every memtable. Caller holds s.mu.
func (s *Store) flushAll() {
	for sid := range s.mem {
		s.flushStream(sid)
	}
}

// checkpoint flushes everything and truncates the WAL. Caller holds s.mu.
func (s *Store) checkpoint() {
	s.flushAll()
	if err := s.wal.Checkpoint(); err != nil {
		s.Log.Error("wal checkpoint", "err", err)
	}
}

func (s *Store) newOpen(sid int64, cluster, key string, ts int64) (*openChunk, error) {
	ns, _, uid, ctr, _ := splitKey(key)
	day := index.Day(ts)
	dir := filepath.Join(s.cfg.Dir, "days", day, cluster, ns, uid, ctr)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, fmt.Sprintf("%019d.open", time.Now().UnixNano()))
	w, err := chunk.Create(p)
	if err != nil {
		return nil, err
	}
	oc := &openChunk{w: w, path: p, day: day, streamID: sid, cluster: cluster, key: key, created: time.Now(), last: time.Now()}
	s.open[sid] = oc
	return oc, nil
}

// seal must be called with s.mu held.
func (s *Store) seal(oc *openChunk) {
	delete(s.open, oc.streamID)
	if oc.w.Lines == 0 {
		oc.w.Close()
		os.Remove(oc.path)
		return
	}
	sealed := strings.TrimSuffix(oc.path, ".open") + ".chunk"
	ft, err := oc.w.Seal(sealed)
	if err != nil {
		s.Log.Error("seal failed", "path", oc.path, "err", err)
		return
	}
	var size int64
	if st, err := os.Stat(sealed); err == nil {
		size = st.Size()
	}
	if err := s.Idx.InsertChunk(index.Chunk{StreamID: oc.streamID, Path: sealed, Day: oc.day, MinTS: ft.MinTS, MaxTS: ft.MaxTS, Lines: ft.Lines, Bytes: size}); err != nil {
		s.Log.Error("index chunk", "path", sealed, "err", err)
	}
}

// Run drives background work: flushing idle memtables, sealing idle chunks, rate sampling, retention.
func (s *Store) Run(ctx context.Context) {
	flushT := time.NewTicker(5 * time.Second)
	sealT := time.NewTicker(15 * time.Second)
	rateT := time.NewTicker(10 * time.Second)
	retT := time.NewTicker(60 * time.Second)
	offT := time.NewTicker(s.cfg.OffloadEvery)
	defer flushT.Stop()
	defer sealT.Stop()
	defer rateT.Stop()
	defer retT.Stop()
	defer offT.Stop()
	var lastB, lastL int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-flushT.C:
			s.mu.Lock()
			for sid, m := range s.mem {
				if time.Since(m.first) >= s.cfg.FlushAge {
					s.flushStream(sid)
				}
			}
			if s.wal.Size() >= s.cfg.WALSegmentBytes {
				s.checkpoint()
			}
			s.mu.Unlock()
		case <-sealT.C:
			s.mu.Lock()
			for _, oc := range s.open {
				if time.Since(oc.last) > s.cfg.MaxOpenAge || (oc.w.Lines == 0 && time.Since(oc.created) > s.cfg.MaxOpenAge) {
					s.seal(oc)
				}
			}
			s.mu.Unlock()
		case <-rateT.C:
			b, l := s.InBytes.Load(), s.InLines.Load()
			s.RateBytes.Store((b - lastB) / 10)
			s.RateLines.Store((l - lastL) / 10)
			lastB, lastL = b, l
		case <-offT.C:
			if s.cfg.Object != nil {
				if err := s.Offload(ctx); err != nil {
					s.Log.Error("offload", "err", err)
				}
			}
		case <-retT.C:
			if err := s.Retention(); err != nil {
				s.Log.Error("retention", "err", err)
			}
		}
	}
}

// ---------- object storage ----------

// relKey is the object key for a chunk path (relative to the data dir).
func (s *Store) relKey(path string) string {
	rel, err := filepath.Rel(s.cfg.Dir, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(rel)
}

// Offload copies sealed chunks older than UploadAfter to object storage.
func (s *Store) Offload(ctx context.Context) error {
	if s.cfg.Object == nil {
		return nil
	}
	cands, err := s.Idx.OffloadCandidates(time.Now().Add(-s.cfg.UploadAfter).UnixNano(), 200)
	if err != nil {
		return err
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b, err := os.ReadFile(c.Path)
		if err != nil {
			continue
		}
		if err := s.cfg.Object.Put(ctx, s.relKey(c.Path), b); err != nil {
			return err
		}
		s.Idx.MarkRemote(c.ID)
	}
	if len(cands) > 0 {
		s.Log.Info("offloaded chunks to object storage", "count", len(cands))
	}
	return nil
}

// evictLocal removes local copies of chunks that live in object storage, oldest first,
// until DiskUsed is under capB. Returns the number evicted.
func (s *Store) evictLocal(capB int64) int {
	n := 0
	for s.DiskUsed() > capB {
		cands, err := s.Idx.EvictCandidates(50)
		if err != nil || len(cands) == 0 {
			break
		}
		for _, c := range cands {
			os.Remove(c.Path)
			s.Idx.MarkLocal(c.ID, false)
			n++
			if s.DiskUsed() <= capB {
				break
			}
		}
	}
	return n
}

// localPath returns a readable path for a chunk, fetching it into the cache when the
// local copy was evicted.
func (s *Store) localPath(ctx context.Context, path string) (string, error) {
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	if s.cfg.Object == nil {
		return "", errors.New("chunk missing locally and no object storage configured")
	}
	key := s.relKey(path)
	cp := filepath.Join(s.cfg.Dir, "cache", filepath.FromSlash(key))
	if _, err := os.Stat(cp); err == nil {
		now := time.Now()
		os.Chtimes(cp, now, now) // LRU touch
		return cp, nil
	}
	b, err := s.cfg.Object.Get(ctx, key, 0, -1)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(cp), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(cp+".tmp", b, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(cp+".tmp", cp); err != nil {
		return "", err
	}
	go s.trimCache()
	return cp, nil
}

// trimCache keeps the fetched-chunk cache under CacheBytes (oldest access first).
func (s *Store) trimCache() {
	root := filepath.Join(s.cfg.Dir, "cache")
	type f struct {
		p string
		t time.Time
		n int64
	}
	var files []f
	var total int64
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if st, e := d.Info(); e == nil {
				files = append(files, f{p, st.ModTime(), st.Size()})
				total += st.Size()
			}
		}
		return nil
	})
	if total <= s.cfg.CacheBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].t.Before(files[j].t) })
	for _, x := range files {
		if total <= s.cfg.CacheBytes {
			break
		}
		os.Remove(x.p)
		total -= x.n
	}
}

// deleteRemote removes object copies for chunks under a path prefix (best effort).
func (s *Store) deleteRemote(prefix string) {
	if s.cfg.Object == nil {
		return
	}
	cs, err := s.Idx.ChunksByPrefix(prefix)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, c := range cs {
		if c.Remote {
			s.cfg.Object.Delete(ctx, s.relKey(c.Path))
		}
	}
}

// ---------- query ----------

// Line is one query result.
type Line struct {
	SID     int64  `json:"sid"`
	TS      int64  `json:"t"`
	Stderr  bool   `json:"e,omitempty"`
	Msg     string `json:"m"`
	Restart int    `json:"r,omitempty"`
}

// Result of a query (newest first).
type Result struct {
	Lines     []Line `json:"lines"`
	Chunks    int    `json:"chunks"`
	Scanned   int64  `json:"scanned_bytes"`
	Millis    int64  `json:"ms"`
	Truncated bool   `json:"truncated"`
	Skipped   int    `json:"skipped_chunks"` // chunks excluded by bloom filter
	Next      int64  `json:"next,omitempty"` // pass as end for the next (older) page
}

type cand struct {
	path    string
	sid     int64
	minTS   int64
	maxTS   int64
	frames  []chunk.FrameRef
	open    bool
	bloom   *bloom.Filter // open chunks: snapshot; sealed: loaded from footer
	mem     []chunk.Entry // memtable snapshot (newest data, not yet in a chunk)
	restart int
}

// Query returns up to limit lines in [start,end], newest first.
func (s *Store) Query(ctx context.Context, sel index.Selector, start, end int64, f *query.Filter, limit int) (Result, error) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	if limit <= 0 {
		limit = 500
	}
	streams, err := s.Idx.ListStreams(ctx, sel, 0)
	if err != nil {
		return Result{}, err
	}
	ids := make([]int64, len(streams))
	for i, st := range streams {
		ids[i] = st.ID
	}
	sealed, err := s.Idx.ChunksFor(ctx, ids, start, end)
	if err != nil {
		return Result{}, err
	}
	var cands []cand
	for _, c := range sealed {
		cands = append(cands, cand{path: c.Path, sid: c.StreamID, minTS: c.MinTS, maxTS: c.MaxTS})
	}
	s.mu.Lock()
	for _, id := range ids {
		if oc := s.open[id]; oc != nil && oc.w.Lines > 0 && oc.w.MinTS <= end && oc.w.MaxTS >= start {
			cands = append(cands, cand{path: oc.path, sid: id, minTS: oc.w.MinTS, maxTS: oc.w.MaxTS, frames: append([]chunk.FrameRef(nil), oc.w.Frames...), open: true, bloom: oc.w.Bloom.Clone()})
		}
		if m := s.mem[id]; m != nil && len(m.entries) > 0 && m.minTS <= end && m.maxTS >= start {
			cands = append(cands, cand{sid: id, minTS: m.minTS, maxTS: m.maxTS, mem: append([]chunk.Entry(nil), m.entries...), restart: m.meta.Restart})
		}
	}
	s.mu.Unlock()
	sort.Slice(cands, func(i, j int) bool { return cands[i].maxTS > cands[j].maxTS })

	res := Result{Lines: []Line{}}
	terms := f.Words()
	var cutoff int64 = -1 << 62 // once we hold `limit` lines, chunks entirely older than this cannot help
	for _, c := range cands {
		if ctx.Err() != nil {
			res.Truncated = true
			break
		}
		if len(res.Lines) >= limit && c.maxTS < cutoff {
			continue
		}
		if res.Scanned > s.cfg.MaxScanBytes {
			res.Truncated = true
			break
		}
		if c.mem != nil { // memtable: small, newest, no bloom
			for i := len(c.mem) - 1; i >= 0; i-- {
				e := c.mem[i]
				res.Scanned += int64(len(e.Msg)) + 13
				if e.TS < start || e.TS > end || !f.Match(e.Msg) {
					continue
				}
				res.Lines = append(res.Lines, Line{SID: c.sid, TS: e.TS, Stderr: e.Stderr, Msg: string(e.Msg), Restart: c.restart})
			}
			if len(res.Lines) >= limit {
				cutoff = kthNewest(res.Lines, limit)
			}
			continue
		}
		frames := c.frames
		if !c.open {
			lp, err := s.localPath(ctx, c.path)
			if err != nil {
				s.Log.Warn("query: chunk unavailable", "path", c.path, "err", err)
				continue
			}
			c.path = lp
			ft, err := chunk.ReadFooter(c.path)
			if err != nil {
				s.Log.Warn("query: bad chunk", "path", c.path, "err", err)
				continue
			}
			frames = ft.Frames
			if ft.Bloom != "" && len(terms) > 0 {
				c.bloom, _ = bloom.Unmarshal(ft.Bloom)
			}
		}
		if c.bloom != nil && !mayContainAll(c.bloom, terms) {
			res.Skipped++
			continue
		}
		fh, err := os.Open(c.path)
		if err != nil {
			continue
		}
		res.Chunks++
		for i := len(frames) - 1; i >= 0; i-- {
			fr := frames[i]
			if fr.MinTS > end || fr.MaxTS < start {
				continue
			}
			if len(res.Lines) >= limit && fr.MaxTS < cutoff {
				continue
			}
			meta, err := chunk.ReadFrame(fh, fr, func(e chunk.Entry) bool {
				res.Scanned += int64(len(e.Msg)) + 13
				if e.TS < start || e.TS > end || !f.Match(e.Msg) {
					return true
				}
				res.Lines = append(res.Lines, Line{SID: c.sid, TS: e.TS, Stderr: e.Stderr, Msg: string(e.Msg)})
				return true
			})
			if err != nil {
				s.Log.Warn("query: bad frame", "path", c.path, "err", err)
				break
			}
			if meta != nil {
				for j := len(res.Lines) - 1; j >= 0 && res.Lines[j].Restart == 0 && res.Lines[j].SID == c.sid; j-- {
					res.Lines[j].Restart = meta.Restart
				}
			}
			if len(res.Lines) >= limit {
				cutoff = kthNewest(res.Lines, limit)
			}
		}
		fh.Close()
	}
	sort.SliceStable(res.Lines, func(i, j int) bool { return res.Lines[i].TS > res.Lines[j].TS })
	if len(res.Lines) > limit {
		res.Lines = res.Lines[:limit]
		res.Truncated = true
	}
	if len(res.Lines) > 0 {
		res.Next = res.Lines[len(res.Lines)-1].TS - 1
	}
	res.Millis = time.Since(t0).Milliseconds()
	return res, nil
}

func mayContainAll(b *bloom.Filter, terms []string) bool {
	for _, t := range terms {
		if !b.MayContain(t) {
			return false
		}
	}
	return true
}

func kthNewest(lines []Line, k int) int64 {
	ts := make([]int64, len(lines))
	for i, l := range lines {
		ts[i] = l.TS
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] > ts[j] })
	return ts[k-1]
}

// ---------- retention ----------

// DiskCap returns the effective disk cap (config, or 90% of the filesystem).
func (s *Store) DiskCap() int64 {
	if s.cfg.MaxDiskBytes > 0 {
		return s.cfg.MaxDiskBytes
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.cfg.Dir, &st); err != nil {
		return 0
	}
	return int64(st.Blocks) * int64(st.Bsize) * 9 / 10
}

// DiskUsed sums sealed chunk bytes plus open chunk sizes plus the WAL.
func (s *Store) DiskUsed() int64 {
	st, _ := s.Idx.Stats()
	s.mu.Lock()
	for _, oc := range s.open {
		st.Bytes += oc.w.Size()
	}
	s.mu.Unlock()
	return st.Bytes + s.wal.TotalSize()
}

// MemStats reports buffered data.
func (s *Store) MemStats() (memBytes int64, memStreams int, walBytes int64) {
	s.mu.Lock()
	memBytes, memStreams = s.memB, len(s.mem)
	s.mu.Unlock()
	return memBytes, memStreams, s.wal.TotalSize()
}

// Retention deletes whole day directories by age, per-namespace overrides, then by disk cap.
func (s *Store) Retention() error {
	root := filepath.Join(s.cfg.Dir, "days")
	ents, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	var days []string
	for _, e := range ents {
		if e.IsDir() && len(e.Name()) == 8 {
			days = append(days, e.Name())
		}
	}
	sort.Strings(days)
	now := time.Now().UTC()
	today := now.Format("20060102")
	dayEnd := func(d string) time.Time {
		t, _ := time.Parse("20060102", d)
		return t.Add(24 * time.Hour)
	}
	// 1. age
	for _, d := range days {
		if now.Sub(dayEnd(d)) > s.cfg.MaxAge && d != today {
			s.deleteDay(d)
		}
	}
	// 2. overrides (only shorter than default make sense)
	for _, ov := range s.cfg.Overrides {
		if ov.MaxAge <= 0 || ov.MaxAge >= s.cfg.MaxAge {
			continue
		}
		for _, d := range days {
			if d == today || now.Sub(dayEnd(d)) <= ov.MaxAge {
				continue
			}
			clusters, _ := os.ReadDir(filepath.Join(root, d))
			for _, c := range clusters {
				nss, _ := os.ReadDir(filepath.Join(root, d, c.Name()))
				for _, n := range nss {
					if globMatch(ov.Match, c.Name()+"/"+n.Name()) {
						p := filepath.Join(root, d, c.Name(), n.Name())
						if s.hasOpenUnder(p) {
							continue
						}
						s.deleteRemote(p + string(filepath.Separator))
						os.RemoveAll(p)
						s.Idx.DeleteChunksByPrefix(p + string(filepath.Separator))
					}
				}
			}
		}
	}
	// 3. disk cap: first drop local copies of chunks that are safe in object storage
	if capB := s.DiskCap(); capB > 0 {
		if n := s.evictLocal(capB); n > 0 {
			s.Log.Info("retention: evicted local copies of offloaded chunks", "count", n)
		}
		for s.DiskUsed() > capB {
			st, _ := s.Idx.Stats()
			ents, _ := os.ReadDir(root)
			var oldest string
			for _, e := range ents {
				if e.IsDir() && len(e.Name()) == 8 && (oldest == "" || e.Name() < oldest) {
					oldest = e.Name()
				}
			}
			if oldest == "" || oldest == today {
				break
			}
			if st.DayBytes[oldest] == 0 && !s.hasOpenUnder(filepath.Join(root, oldest)) {
				break // deleting it frees nothing (WAL/memtable usage is not retention's to reclaim)
			}
			s.deleteDay(oldest)
		}
	}
	// 4. orphan streams
	_, err = s.Idx.DeleteOrphanStreams(now.Add(-24 * time.Hour).UnixNano())
	return err
}

func (s *Store) hasOpenUnder(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, oc := range s.open {
		if strings.HasPrefix(oc.path, prefix) {
			return true
		}
	}
	return false
}

func (s *Store) deleteDay(day string) {
	p := filepath.Join(s.cfg.Dir, "days", day)
	s.mu.Lock()
	for _, oc := range s.open {
		if oc.day == day {
			s.seal(oc)
		}
	}
	s.mu.Unlock()
	s.deleteRemote(p + string(filepath.Separator))
	if err := os.RemoveAll(p); err != nil {
		s.Log.Error("retention: remove day", "day", day, "err", err)
		return
	}
	n, _ := s.Idx.DeleteChunksByPrefix(p + string(filepath.Separator))
	s.Log.Info("retention: deleted day", "day", day, "chunks", n)
}

// globMatch supports '*' wildcards only.
func globMatch(pattern, s string) bool {
	if pattern == "" {
		return false
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for i := 1; i < len(parts); i++ {
		p := parts[i]
		if i == len(parts)-1 {
			return strings.HasSuffix(s, p)
		}
		j := strings.Index(s, p)
		if j < 0 {
			return false
		}
		s = s[j+len(p):]
	}
	return true
}

// ---------- rebuild ----------

// RebuildIndex recreates the catalog from chunk files, then re-derives memtables
// from the WAL so nothing is duplicated or lost.
func (s *Store) RebuildIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Idx.Reset(); err != nil {
		return err
	}
	s.curs = map[string]int64{}
	s.wseq = map[int64]uint64{}
	root := filepath.Join(s.cfg.Dir, "days")
	type cur struct {
		sid  int64
		file string
		off  int64
		ts   int64
	}
	latest := map[int64]cur{}
	n := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".chunk") {
			return nil
		}
		ft, err := chunk.ReadFooter(p)
		if err != nil || ft.Key == "" {
			s.Log.Warn("rebuild: skipping chunk", "path", p, "err", err)
			return nil
		}
		sid, err := s.streamFor(ft.Cluster, ft.Key, "", ft.Restart, ft.MinTS, ft.MaxTS, false, ft.Labels)
		if err != nil {
			return nil
		}
		st, _ := os.Stat(p)
		s.Idx.InsertChunk(index.Chunk{StreamID: sid, Path: p, Day: dayOf(p), MinTS: ft.MinTS, MaxTS: ft.MaxTS, Lines: ft.Lines, Bytes: st.Size()})
		if ft.File != "" && ft.MaxTS >= latest[sid].ts {
			latest[sid] = cur{sid, ft.File, ft.Off, ft.MaxTS}
		}
		if ft.WSeq > s.wseq[sid] {
			s.wseq[sid] = ft.WSeq
			s.Idx.SetWSeq(sid, ft.WSeq)
		}
		n++
		return nil
	})
	if err != nil {
		return err
	}
	// remote-only chunks: list the bucket and read footers with ranged GETs
	if s.cfg.Object != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		objs, err := s.cfg.Object.List(ctx, "days/")
		if err != nil {
			s.Log.Warn("rebuild: list object storage", "err", err)
		}
		remote := 0
		for _, o := range objs {
			if !strings.HasSuffix(o.Key, ".chunk") {
				continue
			}
			p := filepath.Join(s.cfg.Dir, filepath.FromSlash(o.Key))
			if _, err := os.Stat(p); err == nil {
				continue // local copy was already indexed above
			}
			tail, err := s.cfg.Object.Get(ctx, o.Key, max(o.Size-8, 0), -1)
			if err != nil {
				continue
			}
			ft, need, err := chunk.DecodeFooterTail(tail)
			if err == nil && need > 0 {
				tail, err = s.cfg.Object.Get(ctx, o.Key, max(o.Size-int64(need), 0), -1)
				if err == nil {
					ft, _, err = chunk.DecodeFooterTail(tail)
				}
			}
			if err != nil || ft.Key == "" {
				s.Log.Warn("rebuild: bad remote footer", "key", o.Key, "err", err)
				continue
			}
			sid, err := s.streamFor(ft.Cluster, ft.Key, "", ft.Restart, ft.MinTS, ft.MaxTS, false, ft.Labels)
			if err != nil {
				continue
			}
			s.Idx.InsertChunk(index.Chunk{StreamID: sid, Path: p, Day: dayOf(p), MinTS: ft.MinTS, MaxTS: ft.MaxTS, Lines: ft.Lines, Bytes: o.Size, Remote: true, Local: false})
			if ft.File != "" && ft.MaxTS >= latest[sid].ts {
				latest[sid] = cur{sid, ft.File, ft.Off, ft.MaxTS}
			}
			if ft.WSeq > s.wseq[sid] {
				s.wseq[sid] = ft.WSeq
				s.Idx.SetWSeq(sid, ft.WSeq)
			}
			remote++
		}
		if remote > 0 {
			s.Log.Info("rebuild: indexed remote-only chunks", "count", remote)
		}
	}
	for _, c := range latest {
		s.Idx.SetCursor(c.sid, c.file, c.off)
	}
	// open chunks (already loaded) re-register their streams, cursors and wseq
	for sid, oc := range s.open {
		nsid, err := s.streamFor(oc.cluster, oc.key, oc.w.LastMeta().Node, oc.w.Restart, oc.w.MinTS, oc.w.MaxTS, false, oc.w.Labels)
		if err == nil && nsid != sid {
			delete(s.open, sid)
			oc.streamID = nsid
			s.open[nsid] = oc
		}
		if lm := oc.w.LastMeta(); lm != nil {
			s.Idx.SetCursor(oc.streamID, lm.File, lm.Off[1])
		}
		if oc.w.WSeq > s.wseq[oc.streamID] {
			s.wseq[oc.streamID] = oc.w.WSeq
			s.Idx.SetWSeq(oc.streamID, oc.w.WSeq)
		}
	}
	// memtables: discard and re-derive from the WAL with the rebuilt high-water marks
	s.mem = map[int64]*memStream{}
	s.memB = 0
	s.wal.Close()
	replayed := 0
	s.wal, err = wal.Open(filepath.Join(s.cfg.Dir, "wal"), func(r wal.Record) error {
		if s.replay(r) {
			replayed++
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.Log.Info("index rebuilt", "sealed_chunks", n, "open_chunks", len(s.open), "wal_records_buffered", replayed)
	return nil
}

// CheckReport summarises an integrity check of the data directory.
type CheckReport struct {
	SealedChunks int      `json:"sealed_chunks"`
	OpenChunks   int      `json:"open_chunks"`
	BadChunks    []string `json:"bad_chunks,omitempty"`
	Lines        int      `json:"lines"`
	WALRecords   int      `json:"wal_records"`
	IndexStreams int64    `json:"index_streams"`
	IndexChunks  int64    `json:"index_chunks"`
	IndexMissing []string `json:"index_missing_files,omitempty"` // indexed but neither local nor remote
	NotIndexed   []string `json:"not_indexed,omitempty"`         // on disk but not in the index
	OK           bool     `json:"ok"`
}

// Check verifies chunk footers, open-chunk frames, the WAL and the index without
// modifying anything.
func (s *Store) Check() CheckReport {
	var r CheckReport
	root := filepath.Join(s.cfg.Dir, "days")
	onDisk := map[string]bool{}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		switch {
		case strings.HasSuffix(p, ".chunk"):
			onDisk[p] = true
			ft, err := chunk.ReadFooter(p)
			if err != nil {
				r.BadChunks = append(r.BadChunks, p+": "+err.Error())
				return nil
			}
			r.SealedChunks++
			r.Lines += ft.Lines
		case strings.HasSuffix(p, ".open"):
			refs, err := chunk.ScanOpen(p)
			if err != nil {
				r.BadChunks = append(r.BadChunks, p+": "+err.Error())
				return nil
			}
			r.OpenChunks++
			for _, f := range refs {
				r.Lines += f.Count
			}
		}
		return nil
	})
	s.mu.Lock()
	for _, m := range s.mem {
		r.WALRecords++
		r.Lines += len(m.entries)
	}
	s.mu.Unlock()
	st, _ := s.Idx.Stats()
	r.IndexStreams, r.IndexChunks = st.Streams, st.Chunks
	if cs, err := s.Idx.ChunksByPrefix(root); err == nil {
		for _, c := range cs {
			if !onDisk[c.Path] && !c.Remote {
				r.IndexMissing = append(r.IndexMissing, c.Path)
			}
			delete(onDisk, c.Path)
		}
	}
	for p := range onDisk {
		r.NotIndexed = append(r.NotIndexed, p)
	}
	sort.Strings(r.NotIndexed)
	r.OK = len(r.BadChunks) == 0 && len(r.IndexMissing) == 0 && len(r.NotIndexed) == 0
	return r
}

// Uptime since Open.
func (s *Store) Uptime() time.Duration { return time.Since(s.started) }

// OpenChunks returns the number of open chunks.
func (s *Store) OpenChunks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.open)
}
