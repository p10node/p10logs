// Package agent wires discovery, batching and shipping into the p10logs-agent binary.
package agent

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the agent.yaml rendered by the Helm chart.
type Config struct {
	Cluster  string `yaml:"cluster"`
	LogLevel string `yaml:"logLevel"`
	Hub      struct {
		URL       string `yaml:"url"`
		TokenFile string `yaml:"tokenFile"`
		Token     string `yaml:"token"`
		TLS       struct {
			InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
			CAFile             string `yaml:"caFile"`
		} `yaml:"tls"`
	} `yaml:"hub"`
	Paths struct {
		Pods   string `yaml:"pods"`
		Docker string `yaml:"docker"`
		State  string `yaml:"state"`
	} `yaml:"paths"`
	Collect struct {
		IncludeNamespaces []string `yaml:"includeNamespaces"`
		ExcludeNamespaces []string `yaml:"excludeNamespaces"`
		ExcludeContainers []string `yaml:"excludeContainers"`
		Self              bool     `yaml:"self"`
	} `yaml:"collect"`
	Enrich struct {
		Enabled     bool     `yaml:"enabled"`
		Labels      []string `yaml:"labels"`
		Annotations []string `yaml:"annotations"`
	} `yaml:"enrich"`
	Backfill struct {
		Compressed bool `yaml:"compressed"`
	} `yaml:"backfill"`
	HeartbeatInterval time.Duration `yaml:"heartbeatInterval"`
	Ship              struct {
		MaxInFlight int `yaml:"maxInFlight"`
	} `yaml:"ship"`
	Batch struct {
		MaxBytes      int           `yaml:"maxBytes"`
		MaxLines      int           `yaml:"maxLines"`
		FlushInterval time.Duration `yaml:"flushInterval"`
	} `yaml:"batch"`
	Buffer struct {
		MaxBytes int64 `yaml:"maxBytes"`
	} `yaml:"buffer"`
	RateLimit struct {
		LinesPerSecondPerContainer float64 `yaml:"linesPerSecondPerContainer"`
	} `yaml:"rateLimit"`
	Multiline struct {
		Enabled      bool          `yaml:"enabled"`
		StartPattern string        `yaml:"startPattern"`
		MaxLines     int           `yaml:"maxLines"`
		Timeout      time.Duration `yaml:"timeout"`
	} `yaml:"multiline"`
	Health string `yaml:"health"` // listen addr, default :9411
}

// Load reads YAML and applies defaults.
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
	if c.Cluster == "" {
		c.Cluster = "default"
	}
	if c.Paths.Pods == "" {
		c.Paths.Pods = "/var/log/pods"
	}
	if c.Paths.State == "" {
		c.Paths.State = "/var/lib/p10logs"
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.Buffer.MaxBytes == 0 {
		c.Buffer.MaxBytes = 256 << 20
	}
	if c.Multiline.MaxLines == 0 {
		c.Multiline.MaxLines = 200
	}
	if c.Health == "" {
		c.Health = ":9411"
	}
	if v := os.Getenv("P10_HUB_URL"); v != "" {
		c.Hub.URL = v
	}
	if v := os.Getenv("P10_TOKEN"); v != "" {
		c.Hub.Token = v
	}
	return c, nil
}
