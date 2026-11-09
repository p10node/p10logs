package index

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func open(t *testing.T) *DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func stream(cluster, ns, pod, ctr string, last int64) Stream {
	return Stream{Cluster: cluster, Namespace: ns, Pod: pod, UID: "uid-" + pod, Container: ctr, Node: "node-a", LastTS: last}
}

func TestOpenErrors(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "missing", "dir", "index.db")); err == nil {
		t.Fatal("expected error for unwritable path")
	}
}

func TestOpenReopenKeepsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := d.UpsertStream(stream("c", "ns", "p", "app", 10))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.InsertChunk(Chunk{StreamID: id, Path: "days/20240101/c/ns/p/app/1.zst", Day: "20240101", MinTS: 1, MaxTS: 9, Lines: 3, Bytes: 100}); err != nil {
		t.Fatal(err)
	}
	d.Close()
	// second Open runs the ALTER TABLE migrations again; they must be no-ops
	d2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	st, err := d2.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Streams != 1 || st.Chunks != 1 || st.Bytes != 100 {
		t.Fatalf("after reopen %+v", st)
	}
}

func TestUpsertStream(t *testing.T) {
	d := open(t)
	s := stream("prod", "payments", "api-1", "app", 100)
	s.Labels = map[string]string{"app": "api"}
	id, err := d.UpsertStream(s)
	if err != nil || id == 0 {
		t.Fatalf("insert id=%d err=%v", id, err)
	}
	// same key → same id; last_ts/restarts/ended only grow, first_ts set when 0
	s2 := s
	s2.LastTS, s2.FirstTS, s2.Restarts, s2.Ended, s2.Node, s2.Labels = 200, 50, 2, true, "", nil
	id2, err := d.UpsertStream(s2)
	if err != nil || id2 != id {
		t.Fatalf("upsert id=%d want %d err=%v", id2, id, err)
	}
	got := list(t, d, Selector{IDs: []int64{id}})
	if len(got) != 1 {
		t.Fatalf("streams %+v", got)
	}
	g := got[0]
	if g.LastTS != 200 || g.FirstTS != 50 || g.Restarts != 2 || !g.Ended || g.Node != "node-a" || g.Labels["app"] != "api" {
		t.Fatalf("after upsert %+v", g)
	}
	// older values do not regress; smaller first_ts wins; new node and labels replace
	s3 := s
	s3.LastTS, s3.FirstTS, s3.Restarts, s3.Node, s3.Labels = 150, 20, 1, "node-b", map[string]string{"app": "api", "tier": "web"}
	if _, err := d.UpsertStream(s3); err != nil {
		t.Fatal(err)
	}
	g = list(t, d, Selector{IDs: []int64{id}})[0]
	if g.LastTS != 200 || g.FirstTS != 20 || g.Restarts != 2 || !g.Ended || g.Node != "node-b" || g.Labels["tier"] != "web" {
		t.Fatalf("after second upsert %+v", g)
	}
	// larger first_ts does not replace an existing one
	s4 := s
	s4.FirstTS = 999
	d.UpsertStream(s4)
	if g = list(t, d, Selector{IDs: []int64{id}})[0]; g.FirstTS != 20 {
		t.Fatalf("first_ts regressed to %d", g.FirstTS)
	}
	// a different container is a different stream
	other := s
	other.Container = "sidecar"
	id3, err := d.UpsertStream(other)
	if err != nil || id3 == id {
		t.Fatalf("other container id=%d err=%v", id3, err)
	}
}

func list(t *testing.T, d *DB, sel Selector) []Stream {
	t.Helper()
	out, err := d.ListStreams(context.Background(), sel, 0)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWSeq(t *testing.T) {
	d := open(t)
	a, _ := d.UpsertStream(stream("c", "ns", "a", "app", 1))
	b, _ := d.UpsertStream(stream("c", "ns", "b", "app", 1))
	if err := d.SetWSeq(a, 10); err != nil {
		t.Fatal(err)
	}
	d.SetWSeq(a, 5) // lower seq must not win
	d.SetWSeq(b, 7)
	m, err := d.WSeqs()
	if err != nil {
		t.Fatal(err)
	}
	if m[a] != 10 || m[b] != 7 || len(m) != 2 {
		t.Fatalf("wseqs %v", m)
	}
}

func TestCursors(t *testing.T) {
	d := open(t)
	id, _ := d.UpsertStream(stream("c", "ns", "a", "app", 1))
	if off := d.GetCursor(id, "0.log"); off != 0 {
		t.Fatalf("missing cursor = %d", off)
	}
	if err := d.SetCursor(id, "0.log", 100); err != nil {
		t.Fatal(err)
	}
	d.SetCursor(id, "0.log", 60) // out-of-order ack keeps the max
	d.SetCursor(id, "1.log", 5)
	if off := d.GetCursor(id, "0.log"); off != 100 {
		t.Fatalf("cursor 0.log = %d", off)
	}
	if off := d.GetCursor(id, "1.log"); off != 5 {
		t.Fatalf("cursor 1.log = %d", off)
	}
	if off := d.GetCursor(id+1, "0.log"); off != 0 {
		t.Fatalf("cursor of unknown stream = %d", off)
	}
}

func TestChunks(t *testing.T) {
	d := open(t)
	ctx := context.Background()
	a, _ := d.UpsertStream(stream("c", "ns", "a", "app", 1))
	b, _ := d.UpsertStream(stream("c", "ns", "b", "app", 1))
	mk := func(sid int64, path string, min, max int64) {
		if err := d.InsertChunk(Chunk{StreamID: sid, Path: path, Day: Day(min), MinTS: min, MaxTS: max, Lines: 10, Bytes: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	day := int64(24 * time.Hour)
	mk(a, "days/19700101/a/1.zst", 10, 20)
	mk(a, "days/19700101/a/2.zst", 21, 30)
	mk(a, "days/19700102/a/3.zst", day+1, day+10)
	mk(b, "days/19700101/b/1.zst", 15, 25)

	if got, err := d.ChunksFor(ctx, nil, 0, 100); err != nil || got != nil {
		t.Fatalf("no ids → %v %v", got, err)
	}
	// overlap [22,100] of stream a: chunks 2 and 3 (newest first), not chunk 1
	got, err := d.ChunksFor(ctx, []int64{a}, 22, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Path != "days/19700101/a/2.zst" {
		t.Fatalf("ChunksFor(a,22,100) = %+v", got)
	}
	got, _ = d.ChunksFor(ctx, []int64{a, b}, 0, day+100)
	if len(got) != 4 || got[0].Path != "days/19700102/a/3.zst" || got[3].MaxTS != 20 {
		t.Fatalf("all chunks newest first: %+v", got)
	}
	for _, c := range got {
		if !c.Local || c.Remote {
			t.Fatalf("defaults: %+v", c)
		}
	}
	// touching the boundary counts as overlap
	if got, _ = d.ChunksFor(ctx, []int64{a}, 30, 30); len(got) != 1 || got[0].MinTS != 21 {
		t.Fatalf("boundary overlap %+v", got)
	}
	if got, _ = d.ChunksFor(ctx, []int64{b}, 26, 100); len(got) != 0 {
		t.Fatalf("no overlap %+v", got)
	}

	// re-inserting a path replaces the row instead of duplicating it
	if err := d.InsertChunk(Chunk{StreamID: a, Path: "days/19700101/a/1.zst", Day: "19700101", MinTS: 10, MaxTS: 20, Lines: 99, Bytes: 5}); err != nil {
		t.Fatal(err)
	}
	byp, err := d.ChunksByPrefix("days/19700101/a/")
	if err != nil {
		t.Fatal(err)
	}
	if len(byp) != 2 {
		t.Fatalf("ChunksByPrefix %+v", byp)
	}
	sort.Slice(byp, func(i, j int) bool { return byp[i].Path < byp[j].Path })
	if byp[0].Lines != 99 || byp[0].Bytes != 5 {
		t.Fatalf("replace did not update: %+v", byp[0])
	}

	// offload: local-only chunks older than cutoff, oldest first, limited
	cand, err := d.OffloadCandidates(26, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(cand) != 2 || cand[0].MaxTS != 20 || cand[1].MaxTS != 25 {
		t.Fatalf("OffloadCandidates %+v", cand)
	}
	if cand, _ = d.OffloadCandidates(day+100, 1); len(cand) != 1 || cand[0].MaxTS != 20 {
		t.Fatalf("limit %+v", cand)
	}
	// nothing to evict before anything is remote
	if ev, _ := d.EvictCandidates(10); len(ev) != 0 {
		t.Fatalf("EvictCandidates before offload %+v", ev)
	}
	if err := d.MarkRemote(cand[0].ID); err != nil {
		t.Fatal(err)
	}
	if cand, _ = d.OffloadCandidates(day+100, 10); len(cand) != 3 {
		t.Fatalf("remote chunk still offload candidate: %+v", cand)
	}
	ev, err := d.EvictCandidates(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ev) != 1 || !ev[0].Remote || !ev[0].Local || ev[0].MaxTS != 20 {
		t.Fatalf("EvictCandidates %+v", ev)
	}
	if err := d.MarkLocal(ev[0].ID, false); err != nil {
		t.Fatal(err)
	}
	if ev, _ = d.EvictCandidates(10); len(ev) != 0 {
		t.Fatalf("evicted chunk still candidate %+v", ev)
	}
	// a remote-only chunk is still returned by queries
	got, _ = d.ChunksFor(ctx, []int64{a}, 10, 20)
	if len(got) != 1 || got[0].Local || !got[0].Remote {
		t.Fatalf("remote-only chunk %+v", got)
	}
	// InsertChunk with Remote && !Local records a remote-only row (rebuild from bucket)
	if err := d.InsertChunk(Chunk{StreamID: b, Path: "days/19700101/b/9.zst", Day: "19700101", MinTS: 40, MaxTS: 50, Lines: 1, Bytes: 10, Remote: true}); err != nil {
		t.Fatal(err)
	}
	got, _ = d.ChunksFor(ctx, []int64{b}, 40, 50)
	if len(got) != 1 || got[0].Local || !got[0].Remote {
		t.Fatalf("remote-only insert %+v", got)
	}
	d.MarkLocal(got[0].ID, true)
	if got, _ = d.ChunksFor(ctx, []int64{b}, 40, 50); !got[0].Local {
		t.Fatalf("MarkLocal(true) %+v", got)
	}

	st, err := d.Stats()
	if err != nil {
		t.Fatal(err)
	}
	// 5 chunks; one remote-only (1 byte row of 5 bytes not local); day totals exclude it
	if st.Streams != 2 || st.Live != 2 || st.Chunks != 5 || st.RemoteChunks != 2 || st.RemoteOnlyChunks != 1 {
		t.Fatalf("stats %+v", st)
	}
	if st.Bytes != 3*1000+10 {
		t.Fatalf("local bytes %d", st.Bytes)
	}
	if len(st.Days) != 2 || st.Days[0] != "19700101" || st.Days[1] != "19700102" {
		t.Fatalf("days %v", st.Days)
	}
	if st.DayBytes["19700101"] != 2*1000+10 || st.DayBytes["19700102"] != 1000 {
		t.Fatalf("day bytes %v", st.DayBytes)
	}

	// delete a day: rows gone, count returned, other day intact
	n, err := d.DeleteChunksByPrefix("days/19700101/")
	if err != nil || n != 4 {
		t.Fatalf("DeleteChunksByPrefix n=%d err=%v", n, err)
	}
	if rest, _ := d.ChunksByPrefix("days/"); len(rest) != 1 || rest[0].Day != "19700102" {
		t.Fatalf("after delete %+v", rest)
	}
}

func TestPrefixEscaping(t *testing.T) {
	d := open(t)
	id, _ := d.UpsertStream(stream("c", "ns", "a", "app", 1))
	for _, p := range []string{"days/2024_01/x.zst", "days/2024x01/y.zst", "days/50%/z.zst", "days/50x/w.zst"} {
		if err := d.InsertChunk(Chunk{StreamID: id, Path: p, Day: "d", MinTS: 1, MaxTS: 2}); err != nil {
			t.Fatal(err)
		}
	}
	// `_` and `%` in the prefix are literals, not LIKE wildcards
	if got, _ := d.ChunksByPrefix("days/2024_01/"); len(got) != 1 || got[0].Path != "days/2024_01/x.zst" {
		t.Fatalf("underscore prefix %+v", got)
	}
	if got, _ := d.ChunksByPrefix("days/50%/"); len(got) != 1 || got[0].Path != "days/50%/z.zst" {
		t.Fatalf("percent prefix %+v", got)
	}
	if n, _ := d.DeleteChunksByPrefix("days/50%"); n != 1 {
		t.Fatalf("delete percent prefix n=%d", n)
	}
	if n, _ := d.DeleteChunksByPrefix("days/2024_"); n != 1 {
		t.Fatalf("delete underscore prefix n=%d", n)
	}
}

func TestGlobToLike(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"", "", false},
		{"*", "", false},
		{"api", "api", true},
		{"api-*", "api-%", true},
		{"*-api-*", "%-api-%", true},
		{"a_b", `a\_b`, true},
		{"50%", `50\%`, true},
		{`back\slash*`, `back\\slash%`, true},
	}
	for _, c := range cases {
		got, ok := globToLike(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("globToLike(%q) = %q,%v want %q,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestListStreamsSelector(t *testing.T) {
	d := open(t)
	ctx := context.Background()
	add := func(s Stream) int64 {
		id, err := d.UpsertStream(s)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	api1 := stream("prod", "payments", "api-1", "app", 300)
	api1.Labels = map[string]string{"app": "api", "tier": "web"}
	api2 := stream("prod", "payments", "api-2", "app", 200)
	api2.Labels = map[string]string{"app": "api"}
	side := stream("prod", "payments", "api-1", "sidecar", 250)
	worker := stream("prod", "shop", "worker_1", "app", 100)
	stag := stream("staging", "payments", "api-1", "app", 400)
	idAPI1, idAPI2, idSide, idWorker, idStag := add(api1), add(api2), add(side), add(worker), add(stag)

	ids := func(ss []Stream) []int64 {
		out := make([]int64, len(ss))
		for i, s := range ss {
			out[i] = s.ID
		}
		return out
	}
	eq := func(name string, got []Stream, want ...int64) {
		t.Helper()
		g := ids(got)
		if len(g) != len(want) {
			t.Fatalf("%s: got %v want %v", name, g, want)
		}
		for i := range g {
			if g[i] != want[i] {
				t.Fatalf("%s: got %v want %v", name, g, want)
			}
		}
	}

	// no filter: everything, newest activity first
	eq("all", list(t, d, Selector{}), idStag, idAPI1, idSide, idAPI2, idWorker)
	// limit keeps the newest
	lim, _ := d.ListStreams(ctx, Selector{}, 2)
	eq("limit", lim, idStag, idAPI1)
	// exact match and "*" (no filter)
	eq("cluster exact", list(t, d, Selector{Cluster: "staging"}), idStag)
	eq("cluster star", list(t, d, Selector{Cluster: "*", Namespace: "shop"}), idWorker)
	// globs
	eq("pod glob", list(t, d, Selector{Cluster: "prod", Pod: "api-*"}), idAPI1, idSide, idAPI2)
	eq("container glob", list(t, d, Selector{Container: "side*"}), idSide)
	eq("ns glob", list(t, d, Selector{Namespace: "pay*", Container: "app"}), idStag, idAPI1, idAPI2)
	// `_` in an exact value is literal (worker_1 matches, nothing else would via LIKE anyway)
	eq("underscore exact", list(t, d, Selector{Pod: "worker_1"}), idWorker)
	eq("underscore no match", list(t, d, Selector{Pod: "workerX1"}))
	// uid, ids, since
	eq("uid", list(t, d, Selector{UID: "uid-api-2"}), idAPI2)
	eq("ids", list(t, d, Selector{IDs: []int64{idWorker, idAPI2}}), idAPI2, idWorker)
	eq("since", list(t, d, Selector{Since: 250}), idStag, idAPI1, idSide)
	// labels: exact json match; key quotes are stripped
	eq("label", list(t, d, Selector{Labels: map[string]string{"app": "api"}}), idAPI1, idAPI2)
	eq("two labels", list(t, d, Selector{Labels: map[string]string{"app": "api", "tier": "web"}}), idAPI1)
	eq("label miss", list(t, d, Selector{Labels: map[string]string{"app": "web"}}))
	eq("label quoted key", list(t, d, Selector{Labels: map[string]string{`ti"er`: "web"}}), idAPI1)
	// allow: viewer scope drops rows after the SQL filter
	eq("allow", list(t, d, Selector{Allow: func(c, ns string) bool { return c == "prod" && ns == "shop" }}), idWorker)
	// ended + labels decoded
	if err := d.MarkEnded(idWorker); err != nil {
		t.Fatal(err)
	}
	got := list(t, d, Selector{IDs: []int64{idWorker, idAPI1}})
	if !got[1].Ended || got[0].Ended || got[0].Labels["tier"] != "web" || got[1].Labels != nil {
		t.Fatalf("ended/labels %+v", got)
	}
	// a canceled context is reported
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.ListStreams(cctx, Selector{}, 0); err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestOrphansAndReset(t *testing.T) {
	d := open(t)
	old, _ := d.UpsertStream(stream("c", "ns", "old", "app", 100))
	kept, _ := d.UpsertStream(stream("c", "ns", "kept", "app", 100))
	fresh, _ := d.UpsertStream(stream("c", "ns", "fresh", "app", 1000))
	d.SetCursor(old, "0.log", 5)
	d.SetCursor(kept, "0.log", 5)
	if err := d.InsertChunk(Chunk{StreamID: kept, Path: "days/x/kept.zst", Day: "x", MinTS: 1, MaxTS: 2}); err != nil {
		t.Fatal(err)
	}
	// old has no chunks and is stale → removed with its cursor; kept has a chunk; fresh is recent
	n, err := d.DeleteOrphanStreams(500)
	if err != nil || n != 1 {
		t.Fatalf("DeleteOrphanStreams n=%d err=%v", n, err)
	}
	if got := list(t, d, Selector{}); len(got) != 2 {
		t.Fatalf("streams after orphan delete %+v", got)
	}
	if d.GetCursor(old, "0.log") != 0 || d.GetCursor(kept, "0.log") != 5 {
		t.Fatal("cursor cleanup wrong")
	}
	_ = fresh
	if n, _ = d.DeleteOrphanStreams(500); n != 0 {
		t.Fatalf("second pass deleted %d", n)
	}
	st, _ := d.Stats()
	if st.Streams != 2 || st.Chunks != 1 {
		t.Fatalf("stats %+v", st)
	}
	if err := d.Reset(); err != nil {
		t.Fatal(err)
	}
	st, _ = d.Stats()
	if st.Streams != 0 || st.Chunks != 0 || st.Bytes != 0 || len(st.Days) != 0 {
		t.Fatalf("after reset %+v", st)
	}
	if d.GetCursor(kept, "0.log") != 0 {
		t.Fatal("cursor survived reset")
	}
	// the index is usable after a reset and ids restart from the schema
	if id, err := d.UpsertStream(stream("c", "ns", "new", "app", 1)); err != nil || id == 0 {
		t.Fatalf("upsert after reset id=%d err=%v", id, err)
	}
}

func TestDayAndItoa(t *testing.T) {
	if Day(0) != "19700101" {
		t.Fatalf("Day(0) = %s", Day(0))
	}
	ts := time.Date(2026, 10, 7, 23, 59, 59, 0, time.UTC).UnixNano()
	if Day(ts) != "20261007" {
		t.Fatalf("Day = %s", Day(ts))
	}
	// local time zone must not leak in: 23:30 UTC-5 is the next UTC day
	loc := time.FixedZone("x", -5*3600)
	if got := Day(time.Date(2026, 1, 1, 23, 30, 0, 0, loc).UnixNano()); got != "20260102" {
		t.Fatalf("Day in zone = %s", got)
	}
	for n, want := range map[int]string{0: "0", -5: "0", 7: "7", 100000: "100000", 123456789: "123456789"} {
		if got := itoa(n); got != want {
			t.Errorf("itoa(%d) = %s want %s", n, got, want)
		}
	}
}
