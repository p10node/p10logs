package federation

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNewFindEnabled(t *testing.T) {
	var nilPeers *Peers
	if nilPeers.Enabled() {
		t.Fatal("nil peers enabled")
	}
	if New(nil).Enabled() {
		t.Fatal("empty peers enabled")
	}
	p := New([]Peer{{Name: "east", URL: "http://east", Token: "t1"}, {Name: "west", URL: "http://west", Token: "t2"}})
	if !p.Enabled() || p.Client == nil || len(p.List) != 2 {
		t.Fatalf("New = %+v", p)
	}
	if got, ok := p.Find("west"); !ok || got.URL != "http://west" || got.Token != "t2" {
		t.Fatalf("Find(west) = %+v %v", got, ok)
	}
	if got, ok := p.Find("north"); ok || got != (Peer{}) {
		t.Fatalf("Find(north) = %+v %v", got, ok)
	}
}

func TestSplitSID(t *testing.T) {
	cases := []struct{ in, peer, rest string }{
		{"12", "", "12"},
		{"east/12", "east", "12"},
		{"east/west/12", "east", "west/12"}, // a peer's own peer id stays intact
		{"/12", "", "/12"},                  // empty peer is not a peer
		{"", "", ""},
		{"east/", "east", ""},
	}
	for _, c := range cases {
		p, r := SplitSID(c.in)
		if p != c.peer || r != c.rest {
			t.Errorf("SplitSID(%q) = %q,%q want %q,%q", c.in, p, r, c.peer, c.rest)
		}
	}
}

func TestRoute(t *testing.T) {
	got := Route([]string{"1,2", "east/5", "", "east/6,west/7", ",3,"})
	want := map[string][]string{"": {"1", "2", "3"}, "east": {"5", "6"}, "west": {"7"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Route = %v want %v", got, want)
	}
	if got := Route(nil); len(got) != 0 {
		t.Fatalf("Route(nil) = %v", got)
	}
}

func TestHops(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if Hops(r) != 0 {
		t.Fatal("missing header should be 0")
	}
	r.Header.Set(HdrHops, "2")
	if Hops(r) != 2 {
		t.Fatalf("Hops = %d", Hops(r))
	}
	r.Header.Set(HdrHops, "junk")
	if Hops(r) != 0 {
		t.Fatalf("junk header = %d", Hops(r))
	}
}

func TestGet(t *testing.T) {
	var calls atomic.Int32
	var last *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		last = r.Clone(context.Background())
		switch r.URL.Path {
		case "/api/v1/streams":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true}`)
		case "/api/v1/forbidden":
			http.Error(w, "nope", http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := New([]Peer{{Name: "east", URL: srv.URL + "/", Token: "secret"}})
	peer, _ := p.Find("east")
	ctx := context.Background()

	q := url.Values{"ns": {"payments"}, "q": {"error 5xx"}}
	resp, err := p.Get(ctx, peer, "/api/v1/streams", q, 0, "application/json")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != `{"ok":true}` {
		t.Fatalf("body %q", body)
	}
	// trailing slash on the peer URL is trimmed, query encoded, headers forwarded, hops incremented
	if last.URL.Path != "/api/v1/streams" || last.URL.Query().Get("q") != "error 5xx" || last.URL.Query().Get("ns") != "payments" {
		t.Fatalf("request url %s", last.URL)
	}
	if last.Header.Get("Authorization") != "Bearer secret" || last.Header.Get(HdrHops) != "1" || last.Header.Get("Accept") != "application/json" {
		t.Fatalf("headers %v", last.Header)
	}

	// no query, no accept
	resp, err = p.Get(ctx, peer, "/api/v1/streams", nil, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if last.URL.RawQuery != "" || last.Header.Get("Accept") != "" || last.Header.Get(HdrHops) != "2" {
		t.Fatalf("second request %s %v", last.URL, last.Header)
	}

	// the last allowed hop still goes out; one more is refused before any network call
	n := calls.Load()
	resp, err = p.Get(ctx, peer, "/api/v1/streams", nil, MaxHops-1, "")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if last.Header.Get(HdrHops) != strconv.Itoa(MaxHops) || calls.Load() != n+1 {
		t.Fatalf("max hop header %q calls %d", last.Header.Get(HdrHops), calls.Load())
	}
	if _, err := p.Get(ctx, peer, "/api/v1/streams", nil, MaxHops, ""); err == nil || !strings.Contains(err.Error(), "too many hops") {
		t.Fatalf("hops=%d err=%v", MaxHops, err)
	}
	if calls.Load() != n+1 {
		t.Fatal("refused hop still reached the peer")
	}

	// non-200 is an error naming the peer and status
	if _, err := p.Get(ctx, peer, "/api/v1/forbidden", nil, 0, ""); err == nil || !strings.Contains(err.Error(), "east") || !strings.Contains(err.Error(), "403") {
		t.Fatalf("403 err=%v", err)
	}
	if _, err := p.Get(ctx, peer, "/missing", nil, 0, ""); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("404 err=%v", err)
	}

	// canceled context and an unreachable peer surface transport errors
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.Get(cctx, peer, "/api/v1/streams", nil, 0, ""); err == nil {
		t.Fatal("canceled context succeeded")
	}
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	if _, err := p.Get(ctx, Peer{Name: "gone", URL: deadURL, Token: "x"}, "/", nil, 0, ""); err == nil {
		t.Fatal("closed server succeeded")
	}
	// a URL that cannot become a request is an error too
	if _, err := p.Get(ctx, Peer{Name: "bad", URL: "http://[::1", Token: "x"}, "/", nil, 0, ""); err == nil {
		t.Fatal("bad url succeeded")
	}
}
