// Package wire defines the agent→hub push format: zstd-compressed NDJSON.
//
//	{"k":"ns/pod/uid/ctr","f":"<file id>","o":[start,end],"r":3,"end":false}   section header
//	{"t":1727683200123456789,"s":"o","m":"..."}                                 entry (s = o|e)
//
// Cluster and node travel in headers: X-P10-Cluster, X-P10-Node, X-P10-Agent, X-P10-Agent-Stats.
package wire

const (
	HdrCluster = "X-P10-Cluster"
	HdrNode    = "X-P10-Node"
	HdrAgent   = "X-P10-Agent"
	HdrStats   = "X-P10-Agent-Stats"
	HdrHops    = "X-P10-Hops"
	PushPath   = "/api/v1/push"
)

// Section opens a run of entries for one stream.
type Section struct {
	Key     string            `json:"k"`
	File    string            `json:"f"`
	Off     [2]int64          `json:"o"`
	Restart int               `json:"r"`
	End     bool              `json:"end,omitempty"`
	Labels  map[string]string `json:"l,omitempty"` // selected pod labels/annotations (enrichment)
}

// Entry is one log line.
type Entry struct {
	T int64  `json:"t"`
	S string `json:"s"`
	M string `json:"m"`
}
