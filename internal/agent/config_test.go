package agent

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func clearEnv(t *testing.T) {
	t.Helper()
	t.Setenv("P10_HUB_URL", "")
	t.Setenv("P10_TOKEN", "")
}

func write(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Cluster != "default" || c.Paths.Pods != "/var/log/pods" || c.Paths.State != "/var/lib/p10logs" || c.Paths.Docker != "" {
		t.Fatalf("defaults %+v", c)
	}
	if c.HeartbeatInterval != 10*time.Second || c.Buffer.MaxBytes != 256<<20 || c.Multiline.MaxLines != 200 || c.Health != ":9411" {
		t.Fatalf("defaults %+v", c)
	}
	if c.Hub.URL != "" || c.Hub.Token != "" || c.Enrich.Enabled || c.Multiline.Enabled || c.Collect.Self {
		t.Fatalf("zero values changed %+v", c)
	}
}

func TestLoadErrors(t *testing.T) {
	clearEnv(t)
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing file should error")
	}
	if _, err := Load(write(t, "cluster: [\n")); err == nil {
		t.Fatal("invalid yaml should error")
	}
	if _, err := Load(write(t, "heartbeatInterval: soon\n")); err == nil {
		t.Fatal("bad duration should error")
	}
}

func TestLoadFull(t *testing.T) {
	clearEnv(t)
	c, err := Load(write(t, `
cluster: prod
logLevel: debug
hub:
  url: https://hub.example.com
  tokenFile: /etc/p10logs/token
  token: inline
  tls:
    insecureSkipVerify: true
    caFile: /etc/p10logs/ca.crt
paths:
  pods: /mnt/pods
  docker: /var/lib/docker/containers
  state: /state
collect:
  includeNamespaces: [payments, shop]
  excludeNamespaces: [kube-system]
  excludeContainers: [istio-proxy]
  self: true
enrich:
  enabled: true
  labels: [app, "app.kubernetes.io/name"]
  annotations: [team]
backfill:
  compressed: true
heartbeatInterval: 1m
ship:
  maxInFlight: 4
batch:
  maxBytes: 1048576
  maxLines: 5000
  flushInterval: 500ms
buffer:
  maxBytes: 1024
rateLimit:
  linesPerSecondPerContainer: 250.5
multiline:
  enabled: true
  startPattern: '^\d{4}-'
  maxLines: 50
  timeout: 2s
health: ":0"
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cluster != "prod" || c.LogLevel != "debug" {
		t.Fatalf("top %+v", c)
	}
	if c.Hub.URL != "https://hub.example.com" || c.Hub.TokenFile != "/etc/p10logs/token" || c.Hub.Token != "inline" || !c.Hub.TLS.InsecureSkipVerify || c.Hub.TLS.CAFile != "/etc/p10logs/ca.crt" {
		t.Fatalf("hub %+v", c.Hub)
	}
	if c.Paths.Pods != "/mnt/pods" || c.Paths.Docker != "/var/lib/docker/containers" || c.Paths.State != "/state" {
		t.Fatalf("paths %+v", c.Paths)
	}
	if len(c.Collect.IncludeNamespaces) != 2 || c.Collect.IncludeNamespaces[1] != "shop" || c.Collect.ExcludeNamespaces[0] != "kube-system" || c.Collect.ExcludeContainers[0] != "istio-proxy" || !c.Collect.Self {
		t.Fatalf("collect %+v", c.Collect)
	}
	if !c.Enrich.Enabled || len(c.Enrich.Labels) != 2 || c.Enrich.Labels[1] != "app.kubernetes.io/name" || c.Enrich.Annotations[0] != "team" {
		t.Fatalf("enrich %+v", c.Enrich)
	}
	if !c.Backfill.Compressed || c.HeartbeatInterval != time.Minute || c.Ship.MaxInFlight != 4 {
		t.Fatalf("backfill/hb/ship %+v", c)
	}
	if c.Batch.MaxBytes != 1<<20 || c.Batch.MaxLines != 5000 || c.Batch.FlushInterval != 500*time.Millisecond || c.Buffer.MaxBytes != 1024 {
		t.Fatalf("batch/buffer %+v %+v", c.Batch, c.Buffer)
	}
	if c.RateLimit.LinesPerSecondPerContainer != 250.5 {
		t.Fatalf("rateLimit %+v", c.RateLimit)
	}
	if !c.Multiline.Enabled || c.Multiline.StartPattern != `^\d{4}-` || c.Multiline.MaxLines != 50 || c.Multiline.Timeout != 2*time.Second {
		t.Fatalf("multiline %+v", c.Multiline)
	}
	if c.Health != ":0" {
		t.Fatalf("health %q", c.Health)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	clearEnv(t)
	p := write(t, "hub:\n  url: https://file.example.com\n  token: file-token\n")
	t.Setenv("P10_HUB_URL", "https://env.example.com")
	t.Setenv("P10_TOKEN", "env-token")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Hub.URL != "https://env.example.com" || c.Hub.Token != "env-token" {
		t.Fatalf("env override %+v", c.Hub)
	}
	// an unset variable leaves the file values alone
	clearEnv(t)
	c, _ = Load(p)
	if c.Hub.URL != "https://file.example.com" || c.Hub.Token != "file-token" {
		t.Fatalf("file values %+v", c.Hub)
	}
}
