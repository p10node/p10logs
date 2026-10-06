// Package auth covers the hub's three credentials: agent ingest tokens, API tokens,
// and the UI session (none / basic / OIDC via github.com/coreos/go-oidc).
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// ClusterToken is an ingest token restricted to some clusters ("*" globs allowed).
type ClusterToken struct {
	Token    string
	Clusters []string
}

// Role scopes what a viewer may read. Empty Clusters/Namespaces mean "*".
type Role struct {
	Name       string
	Users      []string // basic usernames or OIDC emails
	Domains    []string // OIDC email domains
	APITokens  []string // bearer tokens bound to this role
	Clusters   []string
	Namespaces []string
}

// Allow reports whether the role may read a (cluster, namespace).
func (r *Role) Allow(cluster, ns string) bool {
	if r == nil {
		return true
	}
	return globAny(r.Clusters, cluster) && globAny(r.Namespaces, ns)
}

func globAny(pats []string, s string) bool {
	if len(pats) == 0 {
		return true
	}
	for _, p := range pats {
		if Glob(p, s) {
			return true
		}
	}
	return false
}

// Glob matches with '*' wildcards only.
func Glob(pattern, s string) bool {
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

// denyAll is the role of an authenticated identity that matches no configured role.
var denyAll = &Role{Name: "deny", Clusters: []string{"\x00none"}}

// Config for Auth.
type Config struct {
	IngestTokens   []string
	ClusterTokens  []ClusterToken
	APITokens      []string
	Roles          []Role
	Mode           string // none | basic | oidc
	BasicUser      string
	BasicPass      string
	OIDCIssuer     string
	OIDCClientID   string
	OIDCSecret     string
	AllowedEmails  []string
	AllowedDomains []string
	SessionKey     []byte
	SessionTTL     time.Duration
	PublicURL      string // optional; else derived from the request
	// UsersFile holds local users for basic mode (passwords set through the UI). When no
	// BasicPass is configured and the file is empty, admin/admin is seeded with must_change.
	UsersFile string
	// LockPassword makes the configured BasicUser/BasicPass the only credentials: the users
	// file is not opened (so nothing stored there takes precedence), /auth/password is
	// refused and the UI hides the change-password link. For demo hubs where everyone
	// shares one password and nobody may change it. Requires BasicUser and BasicPass.
	LockPassword bool
}

// Auth holds compiled state.
type Auth struct {
	cfg      Config
	verifier *oidc.IDTokenVerifier
	endpoint oauth2.Endpoint
	users    *userStore
}

const cookieName = "p10s"

// New validates config and discovers the OIDC provider when needed.
func New(ctx context.Context, cfg Config) (*Auth, error) {
	if cfg.Mode == "" {
		cfg.Mode = "none"
	}
	if len(cfg.SessionKey) == 0 {
		cfg.SessionKey = make([]byte, 32)
		rand.Read(cfg.SessionKey)
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = 12 * time.Hour
	}
	a := &Auth{cfg: cfg}
	switch cfg.Mode {
	case "none":
	case "basic":
		if cfg.LockPassword && (cfg.BasicUser == "" || cfg.BasicPass == "") {
			return nil, errors.New("auth: lockPassword needs a configured username and password")
		}
		if cfg.UsersFile != "" && !cfg.LockPassword {
			us, err := openUsers(cfg.UsersFile)
			if err != nil {
				return nil, err
			}
			a.users = us
			if cfg.BasicPass == "" && us.empty() {
				if err := us.set(DefaultUser, DefaultPass, true); err != nil {
					return nil, fmt.Errorf("auth: seed default user: %w", err)
				}
			}
		}
		if a.users == nil && (cfg.BasicUser == "" || cfg.BasicPass == "") {
			return nil, errors.New("auth: basic mode needs a users file or username and password")
		}
	case "oidc":
		if cfg.OIDCIssuer == "" || cfg.OIDCClientID == "" || cfg.OIDCSecret == "" {
			return nil, errors.New("auth: oidc mode needs issuerUrl, clientId and client secret")
		}
		p, err := oidc.NewProvider(ctx, cfg.OIDCIssuer)
		if err != nil {
			return nil, err
		}
		a.verifier = p.Verifier(&oidc.Config{ClientID: cfg.OIDCClientID})
		a.endpoint = p.Endpoint()
	default:
		return nil, errors.New("auth: unknown ui mode " + cfg.Mode)
	}
	return a, nil
}

// Mode returns the UI auth mode.
func (a *Auth) Mode() string { return a.cfg.Mode }

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func tokenIn(t string, list []string) bool {
	if t == "" {
		return false
	}
	for _, x := range list {
		if subtle.ConstantTimeCompare([]byte(t), []byte(x)) == 1 {
			return true
		}
	}
	return false
}

// CheckIngest validates an agent push for the given cluster: a global ingest token
// may push as any cluster; a cluster token only as the clusters it lists.
func (a *Auth) CheckIngest(r *http.Request, cluster string) bool {
	t := bearer(r)
	if tokenIn(t, a.cfg.IngestTokens) {
		return true
	}
	for _, ct := range a.cfg.ClusterTokens {
		if subtle.ConstantTimeCompare([]byte(t), []byte(ct.Token)) == 1 {
			return globAny(ct.Clusters, cluster)
		}
	}
	return false
}

func (a *Auth) roleForToken(t string) *Role {
	if t == "" {
		return nil
	}
	for i := range a.cfg.Roles {
		if tokenIn(t, a.cfg.Roles[i].APITokens) {
			return &a.cfg.Roles[i]
		}
	}
	return nil
}

// Role returns the caller's read scope: nil = unrestricted. When roles are configured,
// an identity that matches none of them is denied everything (global apiTokens excepted).
func (a *Auth) Role(r *http.Request) *Role {
	if len(a.cfg.Roles) == 0 {
		return nil
	}
	t := bearer(r)
	if tokenIn(t, a.cfg.APITokens) {
		return nil
	}
	if role := a.roleForToken(t); role != nil {
		return role
	}
	user := strings.ToLower(a.User(r))
	if user == "" || user == "anonymous" || user == "api-token" {
		return denyAll
	}
	for i := range a.cfg.Roles {
		role := &a.cfg.Roles[i]
		for _, u := range role.Users {
			if strings.ToLower(u) == user {
				return role
			}
		}
		if at := strings.LastIndexByte(user, '@'); at > 0 {
			for _, d := range role.Domains {
				if strings.ToLower(d) == user[at+1:] {
					return role
				}
			}
		}
	}
	return denyAll
}

// User returns the authenticated UI/API identity, or "" if none.
func (a *Auth) User(r *http.Request) string {
	if t := bearer(r); tokenIn(t, a.cfg.APITokens) || a.roleForToken(t) != nil {
		return "api-token"
	}
	switch a.cfg.Mode {
	case "none":
		return "anonymous"
	case "basic":
		if u, mustChange := a.session(r); u != "" && !mustChange {
			return u
		}
		if u, p, ok := r.BasicAuth(); ok {
			if ok, mustChange := a.checkLocal(u, p); ok && !mustChange {
				return u
			}
		}
		return ""
	case "oidc":
		if email, _ := a.session(r); email != "" {
			return email
		}
		return ""
	}
	return ""
}

// RequireUI guards UI and read-API routes.
func (a *Auth) RequireUI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.User(r) != "" {
			next.ServeHTTP(w, r)
			return
		}
		switch a.cfg.Mode {
		case "basic":
			if u, mustChange := a.session(r); u != "" && mustChange {
				if strings.HasPrefix(r.URL.Path, "/api/") {
					http.Error(w, `{"error":"password change required: open /auth/setup"}`, http.StatusForbidden)
					return
				}
				http.Redirect(w, r, "/auth/setup?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/") {
				w.Header().Set("WWW-Authenticate", `Basic realm="p10logs"`)
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, "/auth/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
		case "oidc":
			if strings.HasPrefix(r.URL.Path, "/api/") {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
			http.Redirect(w, r, "/auth/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
		default:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}
	})
}

// ---- sessions ----

func (a *Auth) sign(payload string) string {
	m := hmac.New(sha256.New, a.cfg.SessionKey)
	m.Write([]byte(payload))
	return hex.EncodeToString(m.Sum(nil))
}

func (a *Auth) newSession(user string, mustChange bool) string {
	flag := ""
	if mustChange {
		flag = "|mc"
	}
	payload := base64.RawURLEncoding.EncodeToString([]byte(user + "|" + strconv.FormatInt(time.Now().Add(a.cfg.SessionTTL).Unix(), 10) + flag))
	return payload + "." + a.sign(payload)
}

// session returns the cookie identity and whether it still has to change its password.
func (a *Auth) session(r *http.Request) (string, bool) {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return "", false
	}
	return a.verifySession(c.Value)
}

func (a *Auth) verifySession(v string) (string, bool) {
	i := strings.LastIndexByte(v, '.')
	if i < 0 {
		return "", false
	}
	payload, sig := v[:i], v[i+1:]
	if !hmac.Equal([]byte(sig), []byte(a.sign(payload))) {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return "", false
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) < 2 {
		return "", false
	}
	exp, _ := strconv.ParseInt(parts[1], 10, 64)
	if time.Now().Unix() > exp {
		return "", false
	}
	return parts[0], len(parts) > 2 && parts[2] == "mc"
}

func (a *Auth) redirectURL(r *http.Request) string {
	if a.cfg.PublicURL != "" {
		return strings.TrimSuffix(a.cfg.PublicURL, "/") + "/auth/callback"
	}
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/auth/callback"
}

func (a *Auth) oauth(r *http.Request) *oauth2.Config {
	return &oauth2.Config{ClientID: a.cfg.OIDCClientID, ClientSecret: a.cfg.OIDCSecret, Endpoint: a.endpoint,
		RedirectURL: a.redirectURL(r), Scopes: []string{oidc.ScopeOpenID, "email", "profile"}}
}

func secure(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}

// Routes registers /auth/* handlers.
func (a *Auth) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/auth/me", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, mc := a.session(r)
		json.NewEncoder(w).Encode(map[string]any{"user": a.User(r), "mode": a.cfg.Mode, "must_change": mc, "local": a.cfg.Mode == "basic" && a.users != nil})
	})
	mux.HandleFunc("/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure(r)})
		http.Redirect(w, r, "/", http.StatusFound)
	})
	if a.cfg.Mode == "basic" {
		a.localRoutes(mux)
		return
	}
	if a.cfg.Mode != "oidc" {
		return
	}
	mux.HandleFunc("/auth/login", func(w http.ResponseWriter, r *http.Request) {
		next := r.URL.Query().Get("next")
		if next == "" || !strings.HasPrefix(next, "/") {
			next = "/"
		}
		state := make([]byte, 16)
		rand.Read(state)
		st := hex.EncodeToString(state)
		http.SetCookie(w, &http.Cookie{Name: "p10st", Value: st + "|" + a.sign(st) + "|" + base64.RawURLEncoding.EncodeToString([]byte(next)), Path: "/auth", MaxAge: 600, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure(r)})
		http.Redirect(w, r, a.oauth(r).AuthCodeURL(st), http.StatusFound)
	})
	mux.HandleFunc("/auth/callback", func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("p10st")
		if err != nil {
			http.Error(w, "missing state", http.StatusBadRequest)
			return
		}
		parts := strings.SplitN(c.Value, "|", 3)
		if len(parts) != 3 || parts[0] != r.URL.Query().Get("state") || !hmac.Equal([]byte(parts[1]), []byte(a.sign(parts[0]))) {
			http.Error(w, "bad state", http.StatusBadRequest)
			return
		}
		tok, err := a.oauth(r).Exchange(r.Context(), r.URL.Query().Get("code"))
		if err != nil {
			http.Error(w, "token exchange failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		rawID, _ := tok.Extra("id_token").(string)
		idt, err := a.verifier.Verify(r.Context(), rawID)
		if err != nil {
			http.Error(w, "id token invalid: "+err.Error(), http.StatusUnauthorized)
			return
		}
		var claims struct {
			Email    string `json:"email"`
			Verified bool   `json:"email_verified"`
		}
		if err := idt.Claims(&claims); err != nil || claims.Email == "" {
			http.Error(w, "no email claim", http.StatusUnauthorized)
			return
		}
		if !a.allowed(claims.Email) {
			http.Error(w, "email not allowed: "+claims.Email, http.StatusForbidden)
			return
		}
		a.setSession(w, r, claims.Email, false)
		http.SetCookie(w, &http.Cookie{Name: "p10st", Value: "", Path: "/auth", MaxAge: -1})
		next, _ := base64.RawURLEncoding.DecodeString(parts[2])
		if len(next) == 0 || next[0] != '/' {
			next = []byte("/")
		}
		http.Redirect(w, r, string(next), http.StatusFound)
	})
}

func (a *Auth) allowed(email string) bool {
	if len(a.cfg.AllowedEmails) == 0 && len(a.cfg.AllowedDomains) == 0 {
		return true
	}
	e := strings.ToLower(email)
	for _, x := range a.cfg.AllowedEmails {
		if strings.ToLower(x) == e {
			return true
		}
	}
	if i := strings.LastIndexByte(e, '@'); i > 0 {
		for _, d := range a.cfg.AllowedDomains {
			if strings.ToLower(d) == e[i+1:] {
				return true
			}
		}
	}
	return false
}
