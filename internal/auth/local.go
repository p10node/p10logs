package auth

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Local users for ui.mode=basic. The store is a small JSON file on the hub's data volume,
// so a password set through the UI survives restarts and upgrades. First run without a
// configured password seeds "admin" / "admin" with must_change, and the UI refuses to
// show anything until that password has been replaced.

// DefaultUser and DefaultPass are the first-run credentials.
const DefaultUser, DefaultPass = "admin", "admin"

type localUser struct {
	Hash       string    `json:"hash"`
	MustChange bool      `json:"must_change,omitempty"`
	Updated    time.Time `json:"updated"`
}

type userStore struct {
	mu    sync.Mutex
	path  string
	users map[string]*localUser
}

func openUsers(path string) (*userStore, error) {
	s := &userStore{path: path, users: map[string]*localUser{}}
	b, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(b, &s.users); err != nil {
			return nil, fmt.Errorf("users file %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

func (s *userStore) save() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(s.users, "", "  ")
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *userStore) get(name string) *localUser {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.users[strings.ToLower(name)]
}

func (s *userStore) set(name, password string, mustChange bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[strings.ToLower(name)] = &localUser{Hash: HashPassword(password), MustChange: mustChange, Updated: time.Now()}
	return s.save()
}

func (s *userStore) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.users) == 0
}

// HashPassword returns "pbkdf2$<iterations>$<salt>$<key>" (SHA-256, 600k iterations: the
// OWASP 2023 recommendation; ~0.3 s on a small core, fine for interactive logins).
func HashPassword(password string) string {
	salt := make([]byte, 16)
	rand.Read(salt)
	const iter = 600_000
	key, _ := pbkdf2.Key(sha256.New, password, salt, iter, 32)
	return "pbkdf2$" + strconv.Itoa(iter) + "$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(key)
}

// VerifyPassword checks a password against a HashPassword string.
func VerifyPassword(encoded, password string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2" {
		return false
	}
	iter, err := strconv.Atoi(parts[1])
	salt, err2 := base64.RawStdEncoding.DecodeString(parts[2])
	want, err3 := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || err2 != nil || err3 != nil || iter < 1000 || iter > 10_000_000 {
		return false
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, iter, len(want))
	return err == nil && subtle.ConstantTimeCompare(key, want) == 1
}

// checkLocal verifies a username/password against the user store, then the configured
// basic credentials. It returns (ok, mustChange).
func (a *Auth) checkLocal(user, pass string) (bool, bool) {
	if a.users != nil {
		if u := a.users.get(user); u != nil {
			return VerifyPassword(u.Hash, pass), u.MustChange
		}
	}
	if a.cfg.BasicUser != "" && a.cfg.BasicPass != "" &&
		subtle.ConstantTimeCompare([]byte(user), []byte(a.cfg.BasicUser)) == 1 && subtle.ConstantTimeCompare([]byte(pass), []byte(a.cfg.BasicPass)) == 1 {
		return true, false
	}
	return false, false
}

// SetPassword stores a new password for a local user (clears must_change).
func (a *Auth) SetPassword(user, password string) error {
	if a.users == nil {
		return errors.New("no users file configured")
	}
	if len(password) < 8 {
		return errors.New("password must be at least 8 characters")
	}
	if user == "" {
		return errors.New("no user")
	}
	return a.users.set(user, password, false)
}

// ---- pages ----

var pageTpl = template.Must(template.New("p").Parse(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{{.Title}} · p10logs</title>
<style>
:root{--bg:#0f1216;--panel:#151a20;--line:#262e38;--fg:#d7dde5;--muted:#8a95a3;--accent:#5cc8c1;--err:#f0716a;color-scheme:dark}
@media (prefers-color-scheme:light){:root{--bg:#f3f5f7;--panel:#fff;--line:#d5dbe2;--fg:#1c2128;--muted:#5b6673;--accent:#0f8b84;--err:#c8362e;color-scheme:light}}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
form{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:28px 28px 22px;width:min(360px,calc(100vw - 32px));box-sizing:border-box}
h1{font-size:16px;margin:0 0 4px;display:flex;align-items:center;gap:8px}h1 i{width:16px;height:16px;border-radius:4px;background:var(--accent);display:inline-block}
p{margin:0 0 18px;color:var(--muted);font-size:13px}
label{display:block;font-size:12px;color:var(--muted);margin:12px 0 4px}
input{width:100%;box-sizing:border-box;background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:8px 10px;color:var(--fg);font:inherit}
input:focus{outline:2px solid var(--accent);outline-offset:1px;border-color:transparent}
button{margin-top:18px;width:100%;background:var(--accent);color:#08201e;border:0;border-radius:6px;padding:9px;font:inherit;font-weight:600;cursor:pointer}
.err{color:var(--err);font-size:13px;margin-top:12px}.foot{margin-top:14px;font-size:12px;color:var(--muted)}.foot a{color:var(--accent)}
</style></head><body>
<form method="post" action="{{.Action}}" autocomplete="on">
<h1><i></i>{{.Title}}</h1><p>{{.Lead}}</p>
{{if .ShowUser}}<label for="u">Username</label><input id="u" name="user" value="{{.User}}" autocomplete="username" autofocus required>{{else}}<input type="hidden" name="user" value="{{.User}}">{{end}}
{{if .ShowCurrent}}<label for="c">Current password</label><input id="c" name="current" type="password" autocomplete="current-password" required>{{end}}
{{if .ShowNew}}<label for="n">New password <span style="color:var(--muted)">(8+ characters)</span></label><input id="n" name="password" type="password" autocomplete="new-password" minlength="8" required>
<label for="n2">Repeat new password</label><input id="n2" name="password2" type="password" autocomplete="new-password" minlength="8" required>{{else}}<label for="p">Password</label><input id="p" name="password" type="password" autocomplete="current-password" required>{{end}}
<input type="hidden" name="next" value="{{.Next}}">
{{if .Error}}<div class="err">{{.Error}}</div>{{end}}
<button type="submit">{{.Button}}</button>
{{if .Foot}}<div class="foot">{{.Foot}}</div>{{end}}
</form></body></html>`))

type page struct {
	Title, Lead, Action, Button, User, Next, Error string
	ShowUser, ShowCurrent, ShowNew                 bool
	Foot                                           template.HTML
}

func render(w http.ResponseWriter, code int, p page) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	pageTpl.Execute(w, p)
}

func safeNext(v string) string {
	if v == "" || !strings.HasPrefix(v, "/") || strings.HasPrefix(v, "//") || strings.HasPrefix(v, "/auth/") {
		return "/"
	}
	return v
}

func (a *Auth) setSession(w http.ResponseWriter, r *http.Request, user string, mustChange bool) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: a.newSession(user, mustChange), Path: "/", MaxAge: int(a.cfg.SessionTTL.Seconds()), HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure(r)})
}

// localRoutes serves the login, first-run setup and change-password pages for basic mode.
func (a *Auth) localRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		next := safeNext(r.FormValue("next"))
		if r.Method != http.MethodPost {
			if u, _ := a.session(r); u != "" {
				http.Redirect(w, r, next, http.StatusFound)
				return
			}
			render(w, 200, page{Title: "Sign in to p10logs", Lead: a.loginLead(), Action: "/auth/login", Button: "Sign in", ShowUser: true, Next: next})
			return
		}
		user, pass := strings.TrimSpace(r.FormValue("user")), r.FormValue("password")
		ok, mustChange := a.checkLocal(user, pass)
		if !ok {
			time.Sleep(800 * time.Millisecond) // blunt brute-force brake
			render(w, http.StatusUnauthorized, page{Title: "Sign in to p10logs", Lead: a.loginLead(), Action: "/auth/login", Button: "Sign in", ShowUser: true, User: user, Next: next, Error: "Wrong username or password."})
			return
		}
		a.setSession(w, r, user, mustChange)
		if mustChange {
			http.Redirect(w, r, "/auth/setup?next="+url.QueryEscape(next), http.StatusFound)
			return
		}
		http.Redirect(w, r, next, http.StatusFound)
	})
	mux.HandleFunc("/auth/setup", func(w http.ResponseWriter, r *http.Request) { // first run: replace the default password
		user, mustChange := a.session(r)
		next := safeNext(r.FormValue("next"))
		if user == "" {
			http.Redirect(w, r, "/auth/login", http.StatusFound)
			return
		}
		if !mustChange {
			http.Redirect(w, r, next, http.StatusFound)
			return
		}
		p := page{Title: "Welcome — set the admin password", Lead: "This hub is still on the default password. Choose a new one before continuing.", Action: "/auth/setup", Button: "Save and continue", User: user, Next: next, ShowNew: true}
		if r.Method != http.MethodPost {
			render(w, 200, p)
			return
		}
		pw, pw2 := r.FormValue("password"), r.FormValue("password2")
		switch {
		case pw != pw2:
			p.Error = "The two passwords differ."
		case pw == DefaultPass:
			p.Error = "Pick something other than the default."
		default:
			if err := a.SetPassword(user, pw); err != nil {
				p.Error = err.Error()
			}
		}
		if p.Error != "" {
			render(w, http.StatusBadRequest, p)
			return
		}
		a.setSession(w, r, user, false)
		http.Redirect(w, r, next, http.StatusFound)
	})
	mux.HandleFunc("/auth/password", func(w http.ResponseWriter, r *http.Request) {
		user, _ := a.session(r)
		if user == "" {
			http.Redirect(w, r, "/auth/login?next=/auth/password", http.StatusFound)
			return
		}
		p := page{Title: "Change password", Lead: "Signed in as " + user + ".", Action: "/auth/password", Button: "Change password", User: user, Next: "/", ShowCurrent: true, ShowNew: true, Foot: template.HTML(`<a href="/">Back to logs</a>`)}
		if r.Method != http.MethodPost {
			render(w, 200, p)
			return
		}
		if ok, _ := a.checkLocal(user, r.FormValue("current")); !ok {
			p.Error = "Current password is wrong."
		} else if r.FormValue("password") != r.FormValue("password2") {
			p.Error = "The two passwords differ."
		} else if err := a.SetPassword(user, r.FormValue("password")); err != nil {
			p.Error = err.Error()
		}
		if p.Error != "" {
			render(w, http.StatusBadRequest, p)
			return
		}
		a.setSession(w, r, user, false)
		http.Redirect(w, r, "/", http.StatusFound)
	})
}

func (a *Auth) loginLead() string {
	if a.users != nil && a.users.get(DefaultUser) != nil && a.users.get(DefaultUser).MustChange {
		return "First run: sign in with admin / admin, then set a new password."
	}
	return "Persistent, multi-cluster pod logs."
}
