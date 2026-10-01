package store

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/p10node/p10logs/internal/index"
	"github.com/p10node/p10logs/internal/query"
	"github.com/p10node/p10logs/internal/s3"
	"net/http/httptest"
)

func body(key, file string, s, e int64, lines ...string) string {
	var b strings.Builder
	b.WriteString(`{"k":"` + key + `","f":"` + file + `","o":[` + itoa(s) + `,` + itoa(e) + `],"r":0}` + "\n")
	for i, l := range lines {
		b.WriteString(`{"t":` + itoa(1_700_000_000_000_000_000+s*1e9+int64(i)*1e9) + `,"s":"o","m":"` + l + `"}` + "\n")
	}
	return b.String()
}
func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestIngestDedupQueryRecover(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	st, err := Open(Config{Dir: dir, TargetBytes: 1 << 20}, log)
	if err != nil {
		t.Fatal(err)
	}
	key := "payments/api-1/uid-1/app"
	r1, err := st.Ingest("prod", "node-a", strings.NewReader(body(key, "f1", 0, 100, "hello world", "error: boom")))
	if err != nil || r1.Lines != 2 {
		t.Fatalf("ingest %+v %v", r1, err)
	}
	// exact resend → deduped
	r2, _ := st.Ingest("prod", "node-a", strings.NewReader(body(key, "f1", 0, 100, "hello world", "error: boom")))
	if r2.Deduped != 2 || r2.Lines != 0 {
		t.Fatalf("dedup %+v", r2)
	}
	// next range → accepted
	r3, _ := st.Ingest("prod", "node-a", strings.NewReader(body(key, "f1", 100, 200, "third line")))
	if r3.Lines != 1 {
		t.Fatalf("append %+v", r3)
	}
	q := func(s *Store, f string) []Line {
		res, err := s.Query(context.Background(), index.Selector{Cluster: "prod", Pod: "api-*"}, 0, 1<<62, query.Compile(f), 100)
		if err != nil {
			t.Fatal(err)
		}
		return res.Lines
	}
	if l := q(st, ""); len(l) != 3 || l[0].Msg != "third line" {
		t.Fatalf("query all: %+v", l)
	}
	if l := q(st, "error"); len(l) != 1 || l[0].Msg != "error: boom" {
		t.Fatalf("query filter: %+v", l)
	}
	// crash: close without sealing, reopen → recovery from .open + cursor re-derived
	st.Idx.Close()
	st2, err := Open(Config{Dir: dir}, log)
	if err != nil {
		t.Fatal(err)
	}
	if l := q(st2, ""); len(l) != 3 {
		t.Fatalf("after recovery: %+v", l)
	}
	r4, _ := st2.Ingest("prod", "node-a", strings.NewReader(body(key, "f1", 100, 200, "third line")))
	if r4.Deduped != 1 {
		t.Fatalf("cursor not recovered: %+v", r4)
	}
	// flush memtables, seal, then rebuild the index from files only
	st2.mu.Lock()
	st2.flushAll()
	for _, oc := range st2.open {
		st2.seal(oc)
	}
	st2.mu.Unlock()
	if err := st2.RebuildIndex(); err != nil {
		t.Fatal(err)
	}
	if l := q(st2, "third"); len(l) != 1 {
		t.Fatalf("after rebuild: %+v", l)
	}
	r5, _ := st2.Ingest("prod", "node-a", strings.NewReader(body(key, "f1", 100, 200, "third line")))
	if r5.Deduped != 1 {
		t.Fatalf("cursor not rebuilt from footer: %+v", r5)
	}
	// bloom: a word that never occurred skips every sealed chunk without reading it
	res, _ := st2.Query(context.Background(), index.Selector{Cluster: "prod"}, 0, 1<<62, query.Compile("nonexistentword"), 10)
	if res.Skipped == 0 || res.Chunks != 0 || len(res.Lines) != 0 {
		t.Fatalf("bloom skip: %+v", res)
	}
	res, _ = st2.Query(context.Background(), index.Selector{Cluster: "prod"}, 0, 1<<62, query.Compile("boom"), 10)
	if res.Skipped != 0 || len(res.Lines) != 1 {
		t.Fatalf("bloom false negative: %+v", res)
	}
	// labels: ingest with enrichment, select by label
	lb := `{"k":"payments/api-2/uid-2/app","f":"g","o":[0,10],"r":0,"l":{"app":"api","team":"pay"}}` + "\n" + `{"t":1700000000000000000,"s":"o","m":"labelled"}` + "\n"
	if _, err := st2.Ingest("prod", "n", strings.NewReader(lb)); err != nil {
		t.Fatal(err)
	}
	res, _ = st2.Query(context.Background(), index.Selector{Labels: map[string]string{"team": "pay"}}, 0, 1<<62, query.Compile(""), 10)
	if len(res.Lines) != 1 || res.Lines[0].Msg != "labelled" {
		t.Fatalf("label selector: %+v", res)
	}
	st2.Close()
}

func TestWALCrashRecoveryNoDupNoLoss(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := Config{Dir: dir, FlushBytes: 1 << 20, FlushAge: time.Hour} // nothing flushes on its own
	st, err := Open(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	key := "ns/pod/uid/app"
	for i := int64(0); i < 5; i++ {
		if _, err := st.Ingest("c", "n", strings.NewReader(body(key, "f", i*100, i*100+100, "line "+itoa(i)))); err != nil {
			t.Fatal(err)
		}
	}
	// flush 3 of the 5 records' worth: force a flush now, then add 2 more that stay in WAL only
	st.mu.Lock()
	st.flushAll()
	st.mu.Unlock()
	for i := int64(5); i < 7; i++ {
		st.Ingest("c", "n", strings.NewReader(body(key, "f", i*100, i*100+100, "line "+itoa(i))))
	}
	// crash: do not Close (no flush, no checkpoint); just drop the handles
	st.Idx.Close()
	st.wal.Close()
	st2, err := Open(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	res, _ := st2.Query(context.Background(), index.Selector{Cluster: "c"}, 0, 1<<62, query.Compile(""), 100)
	if len(res.Lines) != 7 {
		t.Fatalf("after crash want 7 lines, got %d: %+v", len(res.Lines), res.Lines)
	}
	seen := map[string]int{}
	for _, l := range res.Lines {
		seen[l.Msg]++
	}
	for k, v := range seen {
		if v != 1 {
			t.Fatalf("duplicate %q x%d", k, v)
		}
	}
	// the replayed records must be in the memtable, not re-flushed twice: flush, checkpoint, reopen
	st2.Close()
	st3, err := Open(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	res, _ = st3.Query(context.Background(), index.Selector{Cluster: "c"}, 0, 1<<62, query.Compile(""), 100)
	if len(res.Lines) != 7 {
		t.Fatalf("after clean restart want 7, got %d", len(res.Lines))
	}
	memB, _, walB := st3.MemStats()
	if memB != 0 || walB != 0 {
		t.Fatalf("clean restart should have empty memtable/wal: mem=%d wal=%d", memB, walB)
	}
	// resend of an old range is still deduped after everything
	r, _ := st3.Ingest("c", "n", strings.NewReader(body(key, "f", 600, 700, "line 6")))
	if r.Deduped != 1 {
		t.Fatalf("cursor lost across restarts: %+v", r)
	}
	st3.Close()
}

func TestObjectStoreOffloadEvictFetchRebuild(t *testing.T) {
	srv := httptest.NewServer(s3.NewFake())
	defer srv.Close()
	obj, _ := s3.New(s3.Config{Endpoint: srv.URL, Bucket: "logs", AccessKey: "a", SecretKey: "s", PathStyle: true})
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := Config{Dir: dir, Object: obj, UploadAfter: time.Nanosecond, MaxDiskBytes: 1, MaxAge: 1000000 * time.Hour}
	st, err := Open(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	key := "ns/pod/uid/app"
	st.Ingest("c", "n", strings.NewReader(body(key, "f", 0, 100, "cold line one", "cold line two")))
	st.mu.Lock()
	st.flushAll()
	for _, oc := range st.open {
		st.seal(oc)
	}
	st.mu.Unlock()
	if err := st.Offload(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, _ := st.Idx.Stats()
	if stats.RemoteChunks != 1 {
		t.Fatalf("not offloaded: %+v", stats)
	}
	// disk cap of 1 byte: retention must evict the local copy, not delete the day
	if err := st.Retention(); err != nil {
		t.Fatal(err)
	}
	stats, _ = st.Idx.Stats()
	if stats.RemoteOnlyChunks != 1 || stats.Chunks != 1 {
		t.Fatalf("expected local eviction keeping the chunk: %+v", stats)
	}
	res, _ := st.Query(context.Background(), index.Selector{Cluster: "c"}, 0, 1<<62, query.Compile("cold"), 10)
	if len(res.Lines) != 2 || res.Chunks != 1 {
		t.Fatalf("query should fetch the cold chunk: %+v", res)
	}
	if _, err := os.Stat(dir + "/cache"); err != nil {
		t.Fatal("cache dir missing")
	}
	// lose the index entirely, rebuild from the bucket listing
	st.Close()
	os.Remove(dir + "/index.sqlite")
	os.RemoveAll(dir + "/cache")
	st2, err := Open(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	if err := st2.RebuildIndex(); err != nil {
		t.Fatal(err)
	}
	res, _ = st2.Query(context.Background(), index.Selector{Cluster: "c"}, 0, 1<<62, query.Compile(""), 10)
	if len(res.Lines) != 2 {
		t.Fatalf("after rebuild from bucket: %+v", res)
	}
	rep := st2.Check()
	if !rep.OK || rep.IndexChunks != 1 {
		t.Fatalf("check: %+v", rep)
	}
	st2.Close()
}
