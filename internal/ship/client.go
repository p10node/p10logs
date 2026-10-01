package ship

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/p10node/p10logs/internal/wire"
)

// ClientConfig mirrors agent config hub section.
type ClientConfig struct {
	URL                string
	TokenFile          string
	Token              string
	InsecureSkipVerify bool
	CAFile             string
	Cluster            string
	Node               string
	Version            string
}

// Client pushes batches to the hub.
type Client struct {
	cfg   ClientConfig
	http  *http.Client
	token atomic.Pointer[string]
	Log   *slog.Logger
}

var ErrAuth = errors.New("hub rejected token (401)")
var ErrTooLarge = errors.New("hub rejected batch (413)")

// NewClient builds an HTTP client (HTTP/2 when TLS).
func NewClient(cfg ClientConfig) (*Client, error) {
	tr := &http.Transport{MaxIdleConns: 4, IdleConnTimeout: 90 * time.Second, ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify, MinVersion: tls.VersionTLS12}}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("caFile: no certificates")
		}
		tr.TLSClientConfig.RootCAs = pool
	}
	c := &Client{cfg: cfg, http: &http.Client{Transport: tr, Timeout: 60 * time.Second}}
	c.reloadToken()
	return c, nil
}

func (c *Client) reloadToken() {
	t := c.cfg.Token
	if c.cfg.TokenFile != "" {
		if b, err := os.ReadFile(c.cfg.TokenFile); err == nil {
			t = strings.TrimSpace(string(b))
		}
	}
	c.token.Store(&t)
}

// Push sends one batch body. stats is optional (heartbeat).
func (c *Client) Push(ctx context.Context, body []byte, stats string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.cfg.URL, "/")+wire.PushPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+*c.token.Load())
	req.Header.Set("Content-Type", "application/x-ndjson")
	if len(body) > 0 {
		req.Header.Set("Content-Encoding", "zstd")
	}
	req.Header.Set(wire.HdrCluster, c.cfg.Cluster)
	req.Header.Set(wire.HdrNode, c.cfg.Node)
	req.Header.Set(wire.HdrAgent, c.cfg.Version)
	if stats != "" {
		req.Header.Set(wire.HdrStats, stats)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 204 || resp.StatusCode == 202 || resp.StatusCode == 200:
		return nil
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		c.reloadToken()
		return ErrAuth
	case resp.StatusCode == 413:
		return ErrTooLarge
	default:
		return fmt.Errorf("hub returned %d", resp.StatusCode)
	}
}
