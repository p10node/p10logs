// Package enrich keeps a per-node map of pod UID → selected labels/annotations by
// listing and watching only this node's pods through the Kubernetes API. It uses
// plain net/http against the in-cluster endpoint (no client-go), so it costs one
// long-lived watch per node.
package enrich

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Config selects what to attach.
type Config struct {
	Node        string
	Labels      []string
	Annotations []string
	// Optional overrides (defaults come from the in-cluster environment).
	APIServer string
	Token     string
	CAFile    string
	Log       *slog.Logger
}

type podMeta struct {
	sel map[string]string
}

// Watcher is the in-memory cache.
type Watcher struct {
	cfg    Config
	http   *http.Client
	base   string
	token  string
	mu     sync.RWMutex
	pods   map[string]podMeta // by uid
	Ready  bool
	Events int64
}

// New prepares a watcher; call Run in a goroutine. Returns an error when not in a cluster.
func New(cfg Config) (*Watcher, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	base := cfg.APIServer
	if base == "" {
		if host == "" {
			return nil, errors.New("enrich: not running in a cluster (KUBERNETES_SERVICE_HOST unset)")
		}
		base = "https://" + host + ":" + port
	}
	token := cfg.Token
	if token == "" {
		b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		if err != nil {
			return nil, fmt.Errorf("enrich: service account token: %w", err)
		}
		token = strings.TrimSpace(string(b))
	}
	caFile := cfg.CAFile
	if caFile == "" {
		caFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if pem, err := os.ReadFile(caFile); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		tlsCfg.RootCAs = pool
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Watcher{cfg: cfg, base: base, token: token, pods: map[string]podMeta{},
		http: &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg, ResponseHeaderTimeout: 30 * time.Second}}}, nil
}

// Lookup returns the selected labels/annotations for a pod uid.
func (w *Watcher) Lookup(uid string) map[string]string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.pods[uid].sel
}

type podObj struct {
	Metadata struct {
		UID             string            `json:"uid"`
		ResourceVersion string            `json:"resourceVersion"`
		Labels          map[string]string `json:"labels"`
		Annotations     map[string]string `json:"annotations"`
	} `json:"metadata"`
}

func (w *Watcher) select_(p *podObj) map[string]string {
	out := map[string]string{}
	for _, k := range w.cfg.Labels {
		if v, ok := p.Metadata.Labels[k]; ok {
			out[k] = v
		}
	}
	for _, k := range w.cfg.Annotations {
		if v, ok := p.Metadata.Annotations[k]; ok {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (w *Watcher) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+w.token)
	req.Header.Set("Accept", "application/json")
	resp, err := w.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("%s: %d %s", path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

// list returns the resourceVersion to watch from.
func (w *Watcher) list(ctx context.Context) (string, error) {
	q := url.Values{"fieldSelector": {"spec.nodeName=" + w.cfg.Node}, "limit": {"500"}}
	fresh := map[string]podMeta{}
	var rv string
	for {
		resp, err := w.get(ctx, "/api/v1/pods?"+q.Encode())
		if err != nil {
			return "", err
		}
		var page struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
				Continue        string `json:"continue"`
			} `json:"metadata"`
			Items []podObj `json:"items"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			return "", err
		}
		for i := range page.Items {
			p := &page.Items[i]
			fresh[p.Metadata.UID] = podMeta{sel: w.select_(p)}
		}
		rv = page.Metadata.ResourceVersion
		if page.Metadata.Continue == "" {
			break
		}
		q.Set("continue", page.Metadata.Continue)
	}
	w.mu.Lock()
	w.pods = fresh
	w.Ready = true
	w.mu.Unlock()
	return rv, nil
}

func (w *Watcher) watch(ctx context.Context, rv string) error {
	q := url.Values{"fieldSelector": {"spec.nodeName=" + w.cfg.Node}, "watch": {"1"}, "resourceVersion": {rv}, "allowWatchBookmarks": {"true"}, "timeoutSeconds": {"1800"}}
	resp, err := w.get(ctx, "/api/v1/pods?"+q.Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(bufio.NewReaderSize(resp.Body, 64<<10))
	for {
		var ev struct {
			Type   string          `json:"type"`
			Object json.RawMessage `json:"object"`
		}
		if err := dec.Decode(&ev); err != nil {
			return err
		}
		w.Events++
		switch ev.Type {
		case "ADDED", "MODIFIED":
			var p podObj
			if json.Unmarshal(ev.Object, &p) == nil && p.Metadata.UID != "" {
				w.mu.Lock()
				w.pods[p.Metadata.UID] = podMeta{sel: w.select_(&p)}
				w.mu.Unlock()
			}
		case "DELETED":
			var p podObj
			if json.Unmarshal(ev.Object, &p) == nil {
				// keep for a while: the agent still drains the pod's log files after deletion
				go func(uid string) {
					time.Sleep(2 * time.Minute)
					w.mu.Lock()
					delete(w.pods, uid)
					w.mu.Unlock()
				}(p.Metadata.UID)
			}
		case "ERROR":
			return errors.New("watch error event (likely 410 Gone), relisting")
		}
	}
}

// Run lists then watches until ctx is done, reconnecting with backoff.
func (w *Watcher) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		rv, err := w.list(ctx)
		if err != nil {
			w.cfg.Log.Warn("enrich: list pods", "err", err)
			sleep(ctx, backoff)
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
		if err := w.watch(ctx, rv); err != nil && ctx.Err() == nil {
			w.cfg.Log.Debug("enrich: watch ended", "err", err)
			sleep(ctx, time.Second)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
