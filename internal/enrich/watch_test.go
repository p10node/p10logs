package enrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func pod(uid, name string, labels, ann map[string]string, ownerKind, ownerName string) string {
	meta := map[string]any{"uid": uid, "name": name, "resourceVersion": "1"}
	if labels != nil {
		meta["labels"] = labels
	}
	if ann != nil {
		meta["annotations"] = ann
	}
	if ownerKind != "" {
		meta["ownerReferences"] = []map[string]string{{"kind": ownerKind, "name": ownerName}}
	}
	b, _ := json.Marshal(map[string]any{"metadata": meta})
	return string(b)
}

func parsePod(t *testing.T, s string) *podObj {
	t.Helper()
	var p podObj
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		t.Fatal(err)
	}
	return &p
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestNewOutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := New(Config{Node: "n"}); err == nil || !strings.Contains(err.Error(), "not running in a cluster") {
		t.Fatalf("expected not-in-cluster error, got %v", err)
	}
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		t.Skip("running inside a cluster: service account token present")
	}
	if _, err := New(Config{Node: "n", APIServer: "http://127.0.0.1:1"}); err == nil || !strings.Contains(err.Error(), "service account token") {
		t.Fatalf("expected token error, got %v", err)
	}
}

func TestNewInCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	w, err := New(Config{Node: "n", Token: " tok \n", CAFile: "/nonexistent/ca.crt"})
	if err != nil {
		t.Fatal(err)
	}
	if w.base != "https://10.0.0.1:443" || w.token != " tok \n" || w.cfg.Log == nil || w.Ready || w.Lookup("x") != nil {
		t.Fatalf("watcher: base=%q token=%q ready=%v", w.base, w.token, w.Ready)
	}
	// an explicit server wins over the environment; a readable CA file is loaded without error
	ca := t.TempDir() + "/ca.crt"
	os.WriteFile(ca, []byte("not a pem"), 0o600)
	if w, err = New(Config{Node: "n", APIServer: "http://127.0.0.1:9", Token: "t", CAFile: ca}); err != nil || w.base != "http://127.0.0.1:9" {
		t.Fatalf("explicit server: %v %q", err, w.base)
	}
}

func TestSelect(t *testing.T) {
	w := &Watcher{cfg: Config{Labels: []string{"app", "team"}, Annotations: []string{"note"}}}
	p := parsePod(t, pod("u1", "api-7bd76897d8-4rkn2", map[string]string{"app": "api", "extra": "x"}, map[string]string{"note": "hi", "other": "no"}, "ReplicaSet", "api-7bd76897d8"))
	got := w.select_(p)
	want := map[string]string{OwnerLabel: "Deployment/api", "app": "api", "note": "hi"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("select: got %v want %v", got, want)
	}
	// nothing configured: still the synthetic owner label
	if got = (&Watcher{}).select_(parsePod(t, pod("u2", "lonely", nil, nil, "", ""))); len(got) != 1 || got[OwnerLabel] != "Pod/lonely" {
		t.Fatalf("owner only: %v", got)
	}
}

// fakeAPI is a minimal pods list/watch endpoint.
type fakeAPI struct {
	srv     *httptest.Server
	lists   atomic.Int32
	watches atomic.Int32
	listRV  string
	pages   [][]string // list pages (JSON pod objects); a continue token links them
	events  []string   // watch events written once, then the stream stays open until the client leaves
	status  int        // non-zero: every response fails with this code
}

func newFakeAPI(t *testing.T, f *fakeAPI) *fakeAPI {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/pods", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("Accept") != "application/json" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		q := r.URL.Query()
		if q.Get("fieldSelector") != "spec.nodeName=node1" {
			http.Error(w, "bad field selector "+q.Get("fieldSelector"), 400)
			return
		}
		if f.status != 0 {
			if q.Get("watch") == "" {
				f.lists.Add(1)
			}
			http.Error(w, "boom", f.status)
			return
		}
		if q.Get("watch") == "" {
			n := f.lists.Add(1)
			page := 0
			if c := q.Get("continue"); c != "" {
				fmt.Sscanf(c, "page-%d", &page)
			}
			if q.Get("limit") != "500" {
				http.Error(w, "limit", 400)
				return
			}
			meta := map[string]any{"resourceVersion": fmt.Sprintf("%s-%d", f.listRV, n)}
			if page+1 < len(f.pages) {
				meta["continue"] = fmt.Sprintf("page-%d", page+1)
			}
			items := "[]"
			if page < len(f.pages) {
				items = "[" + strings.Join(f.pages[page], ",") + "]"
			}
			b, _ := json.Marshal(meta)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"metadata":%s,"items":%s}`, b, items)
			return
		}
		f.watches.Add(1)
		if q.Get("allowWatchBookmarks") != "true" || !strings.HasPrefix(q.Get("resourceVersion"), f.listRV+"-") || q.Get("timeoutSeconds") == "" {
			http.Error(w, "bad watch params", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		for _, e := range f.events {
			io.WriteString(w, e+"\n")
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func newWatcher(t *testing.T, f *fakeAPI) *Watcher {
	t.Helper()
	w, err := New(Config{Node: "node1", Labels: []string{"app"}, Annotations: []string{"team"}, APIServer: f.srv.URL, Token: "tok", Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestListWatchRun(t *testing.T) {
	ev := func(typ, obj string) string {
		return fmt.Sprintf(`{"type":%q,"object":%s}`, typ, obj)
	}
	f := newFakeAPI(t, &fakeAPI{listRV: "rv", pages: [][]string{
		{pod("u1", "api-7bd76897d8-4rkn2", map[string]string{"app": "api", "tier": "web"}, map[string]string{"team": "pay"}, "ReplicaSet", "api-7bd76897d8")},
		{pod("u2", "db-0", map[string]string{"app": "db"}, nil, "StatefulSet", "db")},
	}, events: []string{
		ev("ADDED", pod("u3", "job-x", map[string]string{"app": "job"}, nil, "Job", "nightly-29851812")),
		ev("MODIFIED", pod("u1", "api-7bd76897d8-4rkn2", map[string]string{"app": "api-v2"}, nil, "ReplicaSet", "api-7bd76897d8")),
		ev("DELETED", pod("u2", "db-0", map[string]string{"app": "db"}, nil, "StatefulSet", "db")),
		ev("BOOKMARK", `{"metadata":{"resourceVersion":"rv-9"}}`),
		ev("ADDED", `{"metadata":{"name":"no-uid"}}`),
		`{"type":"ADDED","object":"not an object"}`,
	}})
	w := newWatcher(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	waitFor(t, "modified pod", func() bool { return w.Lookup("u1")["app"] == "api-v2" })
	if got := w.Lookup("u1"); got[OwnerLabel] != "Deployment/api" || got["team"] != "" || len(got) != 2 {
		t.Fatalf("u1 after MODIFIED: %v", got)
	}
	if got := w.Lookup("u3"); got["app"] != "job" || got[OwnerLabel] != "CronJob/nightly" {
		t.Fatalf("u3 ADDED: %v", got)
	}
	// deleted pods stay resolvable while the agent drains their files
	if got := w.Lookup("u2"); got["app"] != "db" || got[OwnerLabel] != "StatefulSet/db" {
		t.Fatalf("u2 after DELETED should still resolve: %v", got)
	}
	if w.Lookup("nope") != nil || w.Lookup("") != nil {
		t.Fatal("unknown uid must be nil")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if !w.Ready || w.Events != 6 || f.lists.Load() != 2 || f.watches.Load() != 1 {
		t.Fatalf("ready=%v events=%d lists=%d watches=%d", w.Ready, w.Events, f.lists.Load(), f.watches.Load())
	}
}

func TestRunRelistsAfterWatchError(t *testing.T) {
	f := newFakeAPI(t, &fakeAPI{listRV: "rv", pages: [][]string{{pod("u1", "p", nil, nil, "", "")}},
		events: []string{`{"type":"ERROR","object":{"kind":"Status","code":410}}`}})
	w := newWatcher(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	waitFor(t, "second list after watch error", func() bool { return f.lists.Load() >= 2 })
	cancel()
	<-done
	if w.Lookup("u1")[OwnerLabel] != "Pod/p" || f.watches.Load() < 1 {
		t.Fatalf("state after relist: %v watches=%d", w.Lookup("u1"), f.watches.Load())
	}
}

func TestRunBacksOffOnListError(t *testing.T) {
	f := newFakeAPI(t, &fakeAPI{listRV: "rv", status: 500})
	w := newWatcher(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	w.Run(ctx) // returns when ctx expires, after a few failed lists with growing backoff
	if n := f.lists.Load(); n < 1 || n > 3 || w.Ready || w.Lookup("u1") != nil {
		t.Fatalf("lists=%d ready=%v", n, w.Ready)
	}
	_, err := w.get(context.Background(), "/api/v1/pods?fieldSelector=spec.nodeName%3Dnode1")
	if err == nil || !strings.Contains(err.Error(), "500") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("get error should carry status and body: %v", err)
	}
	if _, err = w.get(context.Background(), "/api/v1/pods?fieldSelector=other"); err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("get 400: %v", err)
	}
	bad := &Watcher{cfg: w.cfg, http: w.http, base: "http://127.0.0.1:1", token: "tok", pods: map[string]podMeta{}}
	if _, err = bad.list(context.Background()); err == nil {
		t.Fatal("list against a closed port must fail")
	}
	if err = bad.watch(context.Background(), "1"); err == nil {
		t.Fatal("watch against a closed port must fail")
	}
}

func TestSleepHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t0 := time.Now()
	sleep(ctx, time.Minute)
	if time.Since(t0) > time.Second {
		t.Fatal("sleep ignored a canceled context")
	}
}
