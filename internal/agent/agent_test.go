package agent

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/p10node/p10logs/internal/wire"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// testConfig returns a config pointing at temp dirs; withLog adds one pod log file to tail.
func testConfig(t *testing.T, hubURL string, withLog bool) *Config {
	t.Helper()
	clearEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cfg.Cluster = "test"
	cfg.Hub.URL = hubURL
	cfg.Hub.Token = "tok"
	cfg.Paths.Pods = filepath.Join(root, "pods")
	cfg.Paths.State = filepath.Join(root, "state")
	cfg.Health = freeAddr(t)
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.Batch.FlushInterval = 50 * time.Millisecond
	if err := os.MkdirAll(cfg.Paths.Pods, 0o755); err != nil {
		t.Fatal(err)
	}
	if withLog {
		dir := filepath.Join(cfg.Paths.Pods, "payments_api-1_uid-1", "app")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		line := "2026-01-01T00:00:00.000000000Z stdout F hello from the agent test\n"
		if err := os.WriteFile(filepath.Join(dir, "0.log"), []byte(line+line), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return cfg
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// runUntil starts Run, waits for ready, cancels and returns Run's error.
func runUntil(t *testing.T, cfg *Config, ready func() bool) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	waitFor(t, "agent activity", func() bool {
		select {
		case err := <-done:
			t.Fatalf("Run returned early: %v", err)
		default:
		}
		return ready()
	})
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not stop after cancel")
		return nil
	}
}

func readPositions(t *testing.T, cfg *Config) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(cfg.Paths.State, "positions.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRunShipsAndHeartbeats(t *testing.T) {
	t.Setenv("NODE_NAME", "node-t")
	var pushes, beats atomic.Int32
	var badHdr atomic.Bool
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wire.PushPath || r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get(wire.HdrCluster) != "test" || r.Header.Get(wire.HdrNode) != "node-t" || r.Header.Get(wire.HdrAgent) != Version {
			badHdr.Store(true)
		}
		body, _ := io.ReadAll(r.Body)
		if st := r.Header.Get(wire.HdrStats); st != "" {
			if !strings.HasPrefix(st, "files=") || !strings.Contains(st, ";spool_bytes=0;dropped=0") {
				badHdr.Store(true)
			}
			beats.Add(1)
		}
		if len(body) > 0 {
			if r.Header.Get("Content-Encoding") != "zstd" {
				badHdr.Store(true)
			}
			pushes.Add(1)
		}
		w.WriteHeader(204)
	}))
	defer hub.Close()
	cfg := testConfig(t, hub.URL, true)

	err := runUntil(t, cfg, func() bool { return pushes.Load() >= 1 && beats.Load() >= 2 })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if badHdr.Load() {
		t.Fatal("hub saw a push or heartbeat with wrong path or headers")
	}
	if pushes.Load() != 1 {
		t.Fatalf("two lines in one file should be one batch, got %d pushes", pushes.Load())
	}
	// the health server is gone after shutdown
	if _, err := http.Get("http://" + cfg.Health + "/healthz"); err == nil {
		t.Fatal("health server still listening after Run returned")
	}
	// the ack advanced and saved the checkpoint for the tailed file; the spool dir was created
	if pos := readPositions(t, cfg); !strings.Contains(pos, "payments_api-1_uid-1/app/0.log") {
		t.Fatalf("positions.json %s", pos)
	}
	if _, err := os.Stat(filepath.Join(cfg.Paths.State, "buffer")); err != nil {
		t.Fatalf("spool dir: %v", err)
	}
}

func TestRunHealthEndpoints(t *testing.T) {
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer hub.Close()
	cfg := testConfig(t, hub.URL, false) // nothing to tail: the counters stay at zero
	cfg.HeartbeatInterval = time.Hour
	codes := map[string]int{}
	var status string
	err := runUntil(t, cfg, func() bool {
		for _, p := range []string{"/healthz", "/readyz", "/status"} {
			resp, err := http.Get("http://" + cfg.Health + p)
			if err != nil {
				return false
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			codes[p] = resp.StatusCode
			if p == "/status" {
				status = string(b)
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if codes["/healthz"] != 204 || codes["/readyz"] != 204 || codes["/status"] != 200 {
		t.Fatalf("codes %v", codes)
	}
	if status != "files=0 lag_bytes=0 spool_bytes=0 dropped=0 sent=0 failed=0\n" {
		t.Fatalf("status %q", status)
	}
}

func TestRunHubUnreachable(t *testing.T) {
	cfg := testConfig(t, "http://127.0.0.1:1", true)
	cfg.HeartbeatInterval = 20 * time.Millisecond // exercises the heartbeat failure path
	spool := filepath.Join(cfg.Paths.State, "buffer", "*.zst")
	// after three failed pushes the batch lands in the disk spool
	err := runUntil(t, cfg, func() bool {
		m, _ := filepath.Glob(spool)
		return len(m) > 0
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// the hub never came back, so the spool survives the shutdown drain
	if m, _ := filepath.Glob(spool); len(m) != 1 {
		t.Fatalf("spool files after shutdown: %v", m)
	}
	// a spooled batch is durable, so its checkpoint is advanced too
	if pos := readPositions(t, cfg); !strings.Contains(pos, "payments_api-1_uid-1/app/0.log") {
		t.Fatalf("positions.json %s", pos)
	}
}

func TestRunErrors(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// every case must fail during setup; a bounded context turns a wrong assumption into a failure instead of a hang
	run := func(cfg *Config) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := Run(ctx, cfg, log)
		if ctx.Err() != nil {
			t.Fatal("Run started instead of failing during setup")
		}
		return err
	}
	cfg := testConfig(t, "http://127.0.0.1:1", false)
	// state path is a file → MkdirAll fails
	blocked := filepath.Join(t.TempDir(), "file")
	os.WriteFile(blocked, nil, 0o600)
	cfg.Paths.State = blocked
	if err := run(cfg); err == nil {
		t.Fatal("expected error for unusable state dir")
	}
	// a missing CA file fails before anything starts
	cfg = testConfig(t, "https://127.0.0.1:1", false)
	cfg.Hub.TLS.CAFile = filepath.Join(t.TempDir(), "missing.crt")
	if err := run(cfg); err == nil {
		t.Fatal("expected error for missing CA file")
	}
	// an unreadable checkpoint (a directory where positions.json should be) is reported;
	// a corrupt but readable file is deliberately tolerated and starts over
	cfg = testConfig(t, "http://127.0.0.1:1", false)
	if err := os.MkdirAll(filepath.Join(cfg.Paths.State, "positions.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := run(cfg); err == nil {
		t.Fatal("expected error for unreadable positions.json")
	}
}
