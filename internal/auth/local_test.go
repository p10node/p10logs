package auth

import (
	"context"
	"encoding/json"
	"io"
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

// Demo mode: with LockPassword the configured password is the only one accepted, whatever
// the users file says, /auth/password is refused and /auth/me reports no local store.
func TestLockPassword(t *testing.T) {
	dir := t.TempDir()
	users := filepath.Join(dir, "users.json")
	// a store left behind by someone who changed the password before the lock
	prev, err := New(context.Background(), Config{Mode: "basic", UsersFile: users})
	if err != nil {
		t.Fatal(err)
	}
	if err := prev.SetPassword("demo", "changed-by-user"); err != nil {
		t.Fatal(err)
	}

	if _, err := New(context.Background(), Config{Mode: "basic", UsersFile: users, LockPassword: true}); err == nil {
		t.Fatal("lock without a configured password must fail")
	}
	a, err := New(context.Background(), Config{Mode: "basic", BasicUser: "demo", BasicPass: "demo-pass-1", UsersFile: users, LockPassword: true})
	if err != nil {
		t.Fatal(err)
	}
	if ok, mc := a.checkLocal("demo", "demo-pass-1"); !ok || mc {
		t.Fatal("configured password must work")
	}
	if ok, _ := a.checkLocal("demo", "changed-by-user"); ok {
		t.Fatal("users file must be ignored when locked")
	}
	if ok, _ := a.checkLocal("admin", "admin"); ok {
		t.Fatal("default must not be seeded")
	}
	if err := a.SetPassword("demo", "another-pass-1"); err == nil {
		t.Fatal("SetPassword must fail when locked")
	}

	mux := http.NewServeMux()
	a.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if r, _ := c.PostForm(srv.URL+"/auth/login", url.Values{"user": {"demo"}, "password": {"demo-pass-1"}}); r.StatusCode != 302 || r.Header.Get("Location") != "/" {
		t.Fatalf("login: %d %s", r.StatusCode, r.Header.Get("Location"))
	}
	if r, _ := c.Get(srv.URL + "/auth/password"); r.StatusCode != 403 {
		t.Fatalf("change page: %d", r.StatusCode)
	} else if b, _ := io.ReadAll(r.Body); strings.Contains(string(b), `name="password"`) || strings.Contains(string(b), "<button") {
		t.Fatal("locked page must not offer a form")
	}
	if r, _ := c.PostForm(srv.URL+"/auth/password", url.Values{"current": {"demo-pass-1"}, "password": {"another-pass-1"}, "password2": {"another-pass-1"}}); r.StatusCode != 403 {
		t.Fatalf("change post: %d", r.StatusCode)
	}
	if ok, _ := a.checkLocal("demo", "another-pass-1"); ok {
		t.Fatal("password changed despite lock")
	}
	r, _ := c.Get(srv.URL + "/auth/me")
	var me struct {
		User  string `json:"user"`
		Local bool   `json:"local"`
	}
	if err := json.NewDecoder(r.Body).Decode(&me); err != nil {
		t.Fatal(err)
	}
	if me.User != "demo" || me.Local {
		t.Fatalf("me: %+v (local must be false so the UI hides the password link)", me)
	}
}
