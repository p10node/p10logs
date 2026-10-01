// Package hub loads hub.yaml (as rendered by the Helm chart).
package hub

import (
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is hub.yaml.
type Config struct {
	LogLevel string `yaml:"logLevel"`
	Listen   string `yaml:"listen"`
	DataDir  string `yaml:"dataDir"`
	Auth     struct {
		IngestTokenFile string   `yaml:"ingestTokenFile"`
		IngestToken     string   `yaml:"ingestToken"`
		IngestTokens    []string `yaml:"ingestTokens"`
		// Per-cluster tokens: each token may only push as the listed clusters ("*" = any).
		ClusterTokens []struct {
			Token     string   `yaml:"token"`
			TokenFile string   `yaml:"tokenFile"`
			Clusters  []string `yaml:"clusters"`
		} `yaml:"clusterTokens"`
		APITokens []string `yaml:"apiTokens"`
		// Roles scope what a viewer may read. No roles = everyone sees everything.
		Roles []struct {
			Name       string   `yaml:"name"`
			Users      []string `yaml:"users"`      // basic usernames or OIDC emails
			Domains    []string `yaml:"domains"`    // OIDC email domains
			APITokens  []string `yaml:"apiTokens"`  // bearer tokens bound to this role
			Clusters   []string `yaml:"clusters"`   // globs, default ["*"]
			Namespaces []string `yaml:"namespaces"` // globs, default ["*"]
		} `yaml:"roles"`
		SessionKeyFile string `yaml:"sessionKeyFile"`
		PublicURL      string `yaml:"publicUrl"`
		UI             struct {
			Mode  string `yaml:"mode"`
			Basic struct {
				UsernameFile string `yaml:"usernameFile"`
				PasswordFile string `yaml:"passwordFile"`
				Username     string `yaml:"username"`
				Password     string `yaml:"password"`
			} `yaml:"basic"`
			OIDC struct {
				IssuerURL        string   `yaml:"issuerUrl"`
				ClientID         string   `yaml:"clientId"`
				ClientSecretFile string   `yaml:"clientSecretFile"`
				ClientSecret     string   `yaml:"clientSecret"`
				AllowedEmails    []string `yaml:"allowedEmails"`
				AllowedDomains   []string `yaml:"allowedDomains"`
			} `yaml:"oidc"`
		} `yaml:"ui"`
	} `yaml:"auth"`
	Storage struct {
		Retention struct {
			MaxAge       time.Duration `yaml:"maxAge"`
			MaxDiskBytes int64         `yaml:"maxDiskBytes"`
			Overrides    []struct {
				Match  string        `yaml:"match"`
				MaxAge time.Duration `yaml:"maxAge"`
			} `yaml:"overrides"`
		} `yaml:"retention"`
		Segment struct {
			TargetBytes int64         `yaml:"targetBytes"`
			MaxOpenAge  time.Duration `yaml:"maxOpenAge"`
			Compression string        `yaml:"compression"`
		} `yaml:"segment"`
		WAL struct {
			SegmentBytes int64 `yaml:"segmentBytes"`
		} `yaml:"wal"`
		Memtable struct {
			FlushBytes int64         `yaml:"flushBytes"`
			FlushAge   time.Duration `yaml:"flushAge"`
			MaxBytes   int64         `yaml:"maxBytes"`
		} `yaml:"memtable"`
		ObjectStore struct {
			Enabled        bool          `yaml:"enabled"`
			Endpoint       string        `yaml:"endpoint"`
			Bucket         string        `yaml:"bucket"`
			Region         string        `yaml:"region"`
			Prefix         string        `yaml:"prefix"`
			PathStyle      bool          `yaml:"pathStyle"`
			UploadAfter    time.Duration `yaml:"uploadAfter"`
			Interval       time.Duration `yaml:"interval"`
			CacheBytes     int64         `yaml:"cacheBytes"`
			CredentialsDir string        `yaml:"credentialsDir"` // files accessKey, secretKey
			AccessKey      string        `yaml:"accessKey"`
			SecretKey      string        `yaml:"secretKey"`
		} `yaml:"objectStore"`
	} `yaml:"storage"`
	Limits struct {
		MaxBatchBytes int64 `yaml:"maxBatchBytes"`
		MaxLineBytes  int   `yaml:"maxLineBytes"`
		PerCluster    struct {
			BytesPerSecond int64 `yaml:"bytesPerSecond"`
		} `yaml:"perCluster"`
	} `yaml:"limits"`
	Query struct {
		MaxConcurrent int           `yaml:"maxConcurrent"`
		MaxScanBytes  int64         `yaml:"maxScanBytes"`
		Timeout       time.Duration `yaml:"timeout"`
		Tail          struct {
			MaxClients                 int `yaml:"maxClients"`
			MaxLinesPerSecondPerClient int `yaml:"maxLinesPerSecondPerClient"`
			BufferLines                int `yaml:"bufferLines"`
		} `yaml:"tail"`
	} `yaml:"query"`
	Federation struct {
		Enabled bool `yaml:"enabled"`
		Peers   []struct {
			Name      string `yaml:"name"`
			URL       string `yaml:"url"`
			TokenFile string `yaml:"tokenFile"`
			Token     string `yaml:"token"`
		} `yaml:"peers"`
	} `yaml:"federation"`
	Metrics struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"metrics"`
}

func readFile(p string) string {
	if p == "" {
		return ""
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// Load reads YAML, resolves *File fields and applies defaults.
func Load(path string) (*Config, error) {
	c := &Config{}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, err
		}
	}
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.DataDir == "" {
		c.DataDir = "/data"
	}
	if v := readFile(c.Auth.IngestTokenFile); v != "" {
		c.Auth.IngestTokens = append(c.Auth.IngestTokens, v)
	}
	if c.Auth.IngestToken != "" {
		c.Auth.IngestTokens = append(c.Auth.IngestTokens, c.Auth.IngestToken)
	}
	if v := os.Getenv("P10_INGEST_TOKEN"); v != "" {
		c.Auth.IngestTokens = append(c.Auth.IngestTokens, v)
	}
	for i := range c.Auth.ClusterTokens {
		if v := readFile(c.Auth.ClusterTokens[i].TokenFile); v != "" {
			c.Auth.ClusterTokens[i].Token = v
		}
	}
	if d := c.Storage.ObjectStore.CredentialsDir; d != "" {
		if v := readFile(d + "/accessKey"); v != "" {
			c.Storage.ObjectStore.AccessKey = v
		}
		if v := readFile(d + "/secretKey"); v != "" {
			c.Storage.ObjectStore.SecretKey = v
		}
	}
	if v := os.Getenv("P10_S3_ACCESS_KEY"); v != "" {
		c.Storage.ObjectStore.AccessKey = v
	}
	if v := os.Getenv("P10_S3_SECRET_KEY"); v != "" {
		c.Storage.ObjectStore.SecretKey = v
	}
	for i := range c.Federation.Peers {
		if v := readFile(c.Federation.Peers[i].TokenFile); v != "" {
			c.Federation.Peers[i].Token = v
		}
	}
	if v := readFile(c.Auth.UI.Basic.UsernameFile); v != "" {
		c.Auth.UI.Basic.Username = v
	}
	if v := readFile(c.Auth.UI.Basic.PasswordFile); v != "" {
		c.Auth.UI.Basic.Password = v
	}
	if v := readFile(c.Auth.UI.OIDC.ClientSecretFile); v != "" {
		c.Auth.UI.OIDC.ClientSecret = v
	}
	if c.Auth.UI.Mode == "" {
		c.Auth.UI.Mode = "none"
	}
	if c.Query.MaxConcurrent == 0 {
		c.Query.MaxConcurrent = 8
	}
	if c.Query.Tail.MaxClients == 0 {
		c.Query.Tail.MaxClients = 200
	}
	if c.Query.Tail.MaxLinesPerSecondPerClient == 0 {
		c.Query.Tail.MaxLinesPerSecondPerClient = 2000
	}
	if c.Query.Tail.BufferLines == 0 {
		c.Query.Tail.BufferLines = 5000
	}
	if c.Limits.MaxBatchBytes == 0 {
		c.Limits.MaxBatchBytes = 8 << 20
	}
	return c, nil
}
