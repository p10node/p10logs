package auth

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

func TestHashVerify(t *testing.T) {
	h := HashPassword("s3cret-pass")
	if !VerifyPassword(h, "s3cret-pass") || VerifyPassword(h, "s3cret-pasS") || VerifyPassword("garbage", "x") {
		t.Fatal("hash/verify")
	}
}

// First run: admin/admin signs in, is forced to /auth/setup, sets a password, API works with
// the new password (cookie and HTTP Basic), the default no longer does, and the store persists.
func TestFirstRunOnboarding(t *testing.T) {
	dir := t.TempDir()
	users := filepath.Join(dir, "users.json")
	newServer := func() (*httptest.Server, *Auth) {
		a, err := New(context.Background(), Config{Mode: "basic", UsersFile: users})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		a.Routes(mux)
		mux.Handle("/", a.RequireUI(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok " + a.User(r))) })))
		return httptest.NewServer(mux), a
	}
	srv, _ := newServer()
	defer srv.Close()
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	get := func(p string) *http.Response { r, _ := c.Get(srv.URL + p); return r }
	post := func(p string, v url.Values) *http.Response { r, _ := c.PostForm(srv.URL+p, v); return r }

	if r := get("/api/v1/status"); r.StatusCode != 401 {
		t.Fatalf("anonymous api: %d", r.StatusCode)
	}
	if r := get("/"); r.StatusCode != 302 || !strings.HasPrefix(r.Header.Get("Location"), "/auth/login") {
		t.Fatalf("anonymous page: %d %s", r.StatusCode, r.Header.Get("Location"))
	}
	if r := post("/auth/login", url.Values{"user": {"admin"}, "password": {"wrong"}}); r.StatusCode != 401 {
		t.Fatalf("wrong password: %d", r.StatusCode)
	}
	r := post("/auth/login", url.Values{"user": {"admin"}, "password": {"admin"}, "next": {"/?sid=1"}})
	if r.StatusCode != 302 || !strings.HasPrefix(r.Header.Get("Location"), "/auth/setup") {
		t.Fatalf("default login should go to setup: %d %s", r.StatusCode, r.Header.Get("Location"))
	}
	if r := get("/api/v1/status"); r.StatusCode != 403 {
		t.Fatalf("api before setup: %d", r.StatusCode)
	}
	if r := get("/"); r.StatusCode != 302 || !strings.HasPrefix(r.Header.Get("Location"), "/auth/setup") {
		t.Fatalf("page before setup: %d", r.StatusCode)
	}
	if r := post("/auth/setup", url.Values{"password": {"admin"}, "password2": {"admin"}}); r.StatusCode != 400 {
		t.Fatalf("default again: %d", r.StatusCode)
	}
	if r := post("/auth/setup", url.Values{"password": {"short"}, "password2": {"short"}}); r.StatusCode != 400 {
		t.Fatalf("short: %d", r.StatusCode)
	}
	r = post("/auth/setup", url.Values{"password": {"new-pass-123"}, "password2": {"new-pass-123"}, "next": {"/?sid=1"}})
	if r.StatusCode != 302 || r.Header.Get("Location") != "/?sid=1" {
		t.Fatalf("setup: %d %s", r.StatusCode, r.Header.Get("Location"))
	}
	if r := get("/"); r.StatusCode != 200 {
		t.Fatalf("page after setup: %d", r.StatusCode)
	}
	// HTTP Basic for scripts: new password works, the default does not
	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/status", nil)
	req.SetBasicAuth("admin", "new-pass-123")
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != 200 {
		t.Fatalf("basic header new pass: %d", r.StatusCode)
	}
	req.SetBasicAuth("admin", "admin")
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != 401 {
		t.Fatalf("basic header default pass: %d", r.StatusCode)
	}
	// change password
	if r := post("/auth/password", url.Values{"current": {"new-pass-123"}, "password": {"third-pass-456"}, "password2": {"third-pass-456"}}); r.StatusCode != 302 {
		t.Fatalf("change: %d", r.StatusCode)
	}
	// a fresh process reads the store: no reseed, new password valid
	srv2, a2 := newServer()
	defer srv2.Close()
	if ok, mc := a2.checkLocal("admin", "third-pass-456"); !ok || mc {
		t.Fatal("persisted password not accepted")
	}
	if ok, _ := a2.checkLocal("admin", "admin"); ok {
		t.Fatal("default password reseeded")
	}
}

// A configured password (Helm secret) keeps working without onboarding.
func TestConfiguredPasswordNoOnboarding(t *testing.T) {
	a, err := New(context.Background(), Config{Mode: "basic", BasicUser: "ops", BasicPass: "from-secret", UsersFile: filepath.Join(t.TempDir(), "u.json")})
	if err != nil {
		t.Fatal(err)
	}
	if ok, mc := a.checkLocal("ops", "from-secret"); !ok || mc {
		t.Fatal("configured password")
	}
	if ok, _ := a.checkLocal("admin", "admin"); ok {
		t.Fatal("default must not be seeded when a password is configured")
	}
}
