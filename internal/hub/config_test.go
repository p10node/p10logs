package hub

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"P10_INGEST_TOKEN", "P10_S3_ACCESS_KEY", "P10_S3_SECRET_KEY"} {
		t.Setenv(k, "")
	}
}

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
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
	if c.Listen != ":8080" || c.DataDir != "/data" || c.Auth.UI.Mode != "none" {
		t.Fatalf("defaults %+v", c)
	}
	if c.Auth.SessionKeyFile != filepath.Join("/data", "auth", "session.key") || c.Auth.UsersFile != filepath.Join("/data", "auth", "users.json") {
		t.Fatalf("auth files %q %q", c.Auth.SessionKeyFile, c.Auth.UsersFile)
	}
	if c.Query.MaxConcurrent != 8 || c.Query.Tail.MaxClients != 200 || c.Query.Tail.MaxLinesPerSecondPerClient != 2000 || c.Query.Tail.BufferLines != 5000 {
		t.Fatalf("query defaults %+v", c.Query)
	}
	if c.Limits.MaxBatchBytes != 8<<20 {
		t.Fatalf("maxBatchBytes %d", c.Limits.MaxBatchBytes)
	}
	if len(c.Auth.IngestTokens) != 0 || c.Storage.ObjectStore.AccessKey != "" {
		t.Fatalf("secrets leaked from env: %+v", c.Auth)
	}
}

func TestLoadErrors(t *testing.T) {
	clearEnv(t)
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing file should error")
	}
	bad := write(t, t.TempDir(), "hub.yaml", "listen: [\n")
	if _, err := Load(bad); err == nil {
		t.Fatal("invalid yaml should error")
	}
	// a wrong type for a typed field is an error, not a silent default
	typed := write(t, t.TempDir(), "hub.yaml", "query:\n  maxConcurrent: many\n")
	if _, err := Load(typed); err == nil {
		t.Fatal("wrong type should error")
	}
}

func TestLoadFull(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	ingestFile := write(t, dir, "ingest", "  from-file \n")
	clusterTok := write(t, dir, "cluster-tok", "spoke-token\n")
	peerTok := write(t, dir, "peer-tok", "peer-token")
	userFile := write(t, dir, "user", "alice\n")
	passFile := write(t, dir, "pass", "s3cret")
	secretFile := write(t, dir, "oidc", "oidc-secret\n")
	creds := filepath.Join(dir, "s3")
	os.Mkdir(creds, 0o700)
	write(t, creds, "accessKey", "AK\n")
	write(t, creds, "secretKey", "SK\n")
	data := filepath.Join(dir, "data")

	cfg := write(t, dir, "hub.yaml", `
logLevel: debug
listen: "127.0.0.1:9999"
dataDir: `+data+`
auth:
  ingestTokenFile: `+ingestFile+`
  ingestToken: inline
  ingestTokens: [listed]
  clusterTokens:
    - tokenFile: `+clusterTok+`
      clusters: [spoke-a, spoke-b]
    - token: literal
      clusters: ["*"]
    - tokenFile: `+filepath.Join(dir, "missing")+`
      token: fallback
  apiTokens: [api-1]
  roles:
    - name: payments
      users: [bob]
      domains: [example.com]
      apiTokens: [role-tok]
      clusters: [prod*]
      namespaces: [payments]
  publicUrl: https://logs.example.com
  ui:
    mode: oidc
    basic:
      usernameFile: `+userFile+`
      passwordFile: `+passFile+`
      username: ignored
      password: ignored
      lockPassword: true
    oidc:
      issuerUrl: https://idp.example.com
      clientId: p10logs
      clientSecretFile: `+secretFile+`
      allowedEmails: [a@example.com]
      allowedDomains: [example.com]
storage:
  retention:
    maxAge: 168h
    maxDiskBytes: 10737418240
    overrides:
      - match: "prod/kube-system"
        maxAge: 24h
  segment:
    targetBytes: 4194304
    maxOpenAge: 30s
    compression: zstd
  wal:
    segmentBytes: 1048576
  memtable:
    flushBytes: 262144
    flushAge: 30s
    maxBytes: 134217728
  objectStore:
    enabled: true
    endpoint: https://s3.example.com
    bucket: logs
    region: us-east-1
    prefix: p10
    pathStyle: true
    uploadAfter: 2h
    interval: 5m
    cacheBytes: 1024
    credentialsDir: `+creds+`
limits:
  maxBatchBytes: 1024
  maxLineBytes: 65536
  perCluster:
    bytesPerSecond: 1000000
query:
  maxConcurrent: 2
  maxScanBytes: 5000
  timeout: 20s
  tail:
    maxClients: 3
    maxLinesPerSecondPerClient: 4
    bufferLines: 5
federation:
  enabled: true
  peers:
    - name: east
      url: https://east.example.com
      tokenFile: `+peerTok+`
    - name: west
      url: https://west.example.com
      token: west-token
metrics:
  enabled: true
`)
	c, err := Load(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if c.LogLevel != "debug" || c.Listen != "127.0.0.1:9999" || c.DataDir != data {
		t.Fatalf("top-level %+v", c)
	}
	// listed tokens first, then file (trimmed), then inline; env absent
	want := []string{"listed", "from-file", "inline"}
	if len(c.Auth.IngestTokens) != 3 {
		t.Fatalf("ingestTokens %v", c.Auth.IngestTokens)
	}
	for i := range want {
		if c.Auth.IngestTokens[i] != want[i] {
			t.Fatalf("ingestTokens %v want %v", c.Auth.IngestTokens, want)
		}
	}
	ct := c.Auth.ClusterTokens
	if len(ct) != 3 || ct[0].Token != "spoke-token" || ct[1].Token != "literal" || ct[2].Token != "fallback" {
		t.Fatalf("clusterTokens %+v", ct)
	}
	if len(ct[0].Clusters) != 2 || ct[0].Clusters[1] != "spoke-b" || ct[1].Clusters[0] != "*" {
		t.Fatalf("clusterTokens clusters %+v", ct)
	}
	if len(c.Auth.APITokens) != 1 || c.Auth.APITokens[0] != "api-1" {
		t.Fatalf("apiTokens %v", c.Auth.APITokens)
	}
	r := c.Auth.Roles
	if len(r) != 1 || r[0].Name != "payments" || r[0].Users[0] != "bob" || r[0].Domains[0] != "example.com" || r[0].APITokens[0] != "role-tok" || r[0].Clusters[0] != "prod*" || r[0].Namespaces[0] != "payments" {
		t.Fatalf("roles %+v", r)
	}
	if c.Auth.PublicURL != "https://logs.example.com" {
		t.Fatalf("publicUrl %q", c.Auth.PublicURL)
	}
	// files override inline values; derived paths follow dataDir
	if c.Auth.UI.Mode != "oidc" || c.Auth.UI.Basic.Username != "alice" || c.Auth.UI.Basic.Password != "s3cret" || !c.Auth.UI.Basic.LockPassword {
		t.Fatalf("basic %+v", c.Auth.UI.Basic)
	}
	o := c.Auth.UI.OIDC
	if o.IssuerURL != "https://idp.example.com" || o.ClientID != "p10logs" || o.ClientSecret != "oidc-secret" || o.AllowedEmails[0] != "a@example.com" || o.AllowedDomains[0] != "example.com" {
		t.Fatalf("oidc %+v", o)
	}
	if c.Auth.SessionKeyFile != filepath.Join(data, "auth", "session.key") || c.Auth.UsersFile != filepath.Join(data, "auth", "users.json") {
		t.Fatalf("auth files %q %q", c.Auth.SessionKeyFile, c.Auth.UsersFile)
	}
	s := c.Storage
	if s.Retention.MaxAge != 168*time.Hour || s.Retention.MaxDiskBytes != 10<<30 || len(s.Retention.Overrides) != 1 || s.Retention.Overrides[0].Match != "prod/kube-system" || s.Retention.Overrides[0].MaxAge != 24*time.Hour {
		t.Fatalf("retention %+v", s.Retention)
	}
	if s.Segment.TargetBytes != 4<<20 || s.Segment.MaxOpenAge != 30*time.Second || s.Segment.Compression != "zstd" || s.WAL.SegmentBytes != 1<<20 {
		t.Fatalf("segment/wal %+v %+v", s.Segment, s.WAL)
	}
	if s.Memtable.FlushBytes != 256<<10 || s.Memtable.FlushAge != 30*time.Second || s.Memtable.MaxBytes != 128<<20 {
		t.Fatalf("memtable %+v", s.Memtable)
	}
	os3 := s.ObjectStore
	if !os3.Enabled || os3.Endpoint != "https://s3.example.com" || os3.Bucket != "logs" || os3.Region != "us-east-1" || os3.Prefix != "p10" || !os3.PathStyle || os3.UploadAfter != 2*time.Hour || os3.Interval != 5*time.Minute || os3.CacheBytes != 1024 {
		t.Fatalf("objectStore %+v", os3)
	}
	if os3.AccessKey != "AK" || os3.SecretKey != "SK" {
		t.Fatalf("credentialsDir not resolved: %+v", os3)
	}
	if c.Limits.MaxBatchBytes != 1024 || c.Limits.MaxLineBytes != 65536 || c.Limits.PerCluster.BytesPerSecond != 1_000_000 {
		t.Fatalf("limits %+v", c.Limits)
	}
	if c.Query.MaxConcurrent != 2 || c.Query.MaxScanBytes != 5000 || c.Query.Timeout != 20*time.Second || c.Query.Tail.MaxClients != 3 || c.Query.Tail.MaxLinesPerSecondPerClient != 4 || c.Query.Tail.BufferLines != 5 {
		t.Fatalf("query %+v", c.Query)
	}
	f := c.Federation
	if !f.Enabled || len(f.Peers) != 2 || f.Peers[0].Name != "east" || f.Peers[0].URL != "https://east.example.com" || f.Peers[0].Token != "peer-token" || f.Peers[1].Token != "west-token" {
		t.Fatalf("federation %+v", f)
	}
	if !c.Metrics.Enabled {
		t.Fatal("metrics")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	creds := filepath.Join(dir, "s3")
	os.Mkdir(creds, 0o700)
	write(t, creds, "accessKey", "file-ak")
	cfg := write(t, dir, "hub.yaml", `
auth:
  ingestToken: inline
storage:
  objectStore:
    credentialsDir: `+creds+`
    accessKey: inline-ak
    secretKey: inline-sk
`)
	t.Setenv("P10_INGEST_TOKEN", "from-env")
	t.Setenv("P10_S3_ACCESS_KEY", "env-ak")
	t.Setenv("P10_S3_SECRET_KEY", "env-sk")
	c, err := Load(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Auth.IngestTokens) != 2 || c.Auth.IngestTokens[0] != "inline" || c.Auth.IngestTokens[1] != "from-env" {
		t.Fatalf("ingestTokens %v", c.Auth.IngestTokens)
	}
	// env beats the credentials dir, which beats inline; a missing secretKey file keeps inline until env
	if c.Storage.ObjectStore.AccessKey != "env-ak" || c.Storage.ObjectStore.SecretKey != "env-sk" {
		t.Fatalf("s3 creds %+v", c.Storage.ObjectStore)
	}
	t.Setenv("P10_S3_ACCESS_KEY", "")
	t.Setenv("P10_S3_SECRET_KEY", "")
	c, _ = Load(cfg)
	if c.Storage.ObjectStore.AccessKey != "file-ak" || c.Storage.ObjectStore.SecretKey != "inline-sk" {
		t.Fatalf("s3 creds without env %+v", c.Storage.ObjectStore)
	}
}

func TestReadFile(t *testing.T) {
	if readFile("") != "" {
		t.Fatal("empty path")
	}
	if readFile(filepath.Join(t.TempDir(), "nope")) != "" {
		t.Fatal("missing file")
	}
	p := write(t, t.TempDir(), "tok", "\n\t  value with space \n\n")
	if got := readFile(p); got != "value with space" {
		t.Fatalf("readFile = %q", got)
	}
}
