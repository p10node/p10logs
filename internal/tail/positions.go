// Package tail discovers and follows CRI log files on a node.
package tail

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Pos is the checkpoint for one (path, inode).
type Pos struct {
	Offset int64  `json:"offset"`
	Inode  uint64 `json:"inode"`
	Done   bool   `json:"done,omitempty"` // fully read and file gone / gz backfilled
	Seen   int64  `json:"seen"`           // unix seconds, for purging
}

// Positions is a crash-safe JSON checkpoint file (atomic rename on save).
type Positions struct {
	mu    sync.Mutex
	path  string
	m     map[string]Pos
	dirty bool
}

// Key builds the legacy map key from a path and its inode (kept for migration and as the
// prefix of FileKey).
func Key(path string, inode uint64) string { return path + "@" + itoa(inode) }

// FileKey identifies one file's *content*: path, inode and a hash of its first line.
// Inode alone is not an identity over time: kubelet deletes a rotated file after gzipping
// it and the kernel hands the freed inode to the next 0.log, which would then inherit the
// old file's checkpoint and the hub's cursor (every new line judged a duplicate). The first
// CRI line carries a runtime timestamp, so it is unique per file and changes on truncation.
func FileKey(path string, inode uint64, firstLine []byte) string {
	var h uint64 = 14695981039346656037
	for _, b := range firstLine {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return Key(path, inode) + "@" + strconv.FormatUint(h, 36)
}

func itoa(u uint64) string {
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
		if u == 0 {
			break
		}
	}
	return string(b[i:])
}

// LoadPositions reads the file (missing file = empty).
func LoadPositions(path string) (*Positions, error) {
	p := &Positions{path: path, m: map[string]Pos{}}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return p, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(b, &p.m); err != nil {
		// corrupt checkpoint: start over rather than refuse to run (dedup covers duplicates)
		p.m = map[string]Pos{}
	}
	return p, nil
}

// Get returns the checkpoint.
func (p *Positions) Get(key string) (Pos, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.m[key]
	return v, ok
}

// Set records a checkpoint.
func (p *Positions) Set(key string, pos Pos) {
	p.mu.Lock()
	pos.Seen = time.Now().Unix()
	p.m[key] = pos
	p.dirty = true
	p.mu.Unlock()
}

// HasAnyWithPrefix reports whether any key starts with prefix (used to detect first discovery of a container dir).
func (p *Positions) HasAnyWithPrefix(prefix string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k := range p.m {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// Save writes the file if dirty. Entries not seen for 24h are purged.
func (p *Positions) Save() error {
	p.mu.Lock()
	if !p.dirty {
		p.mu.Unlock()
		return nil
	}
	cut := time.Now().Add(-24 * time.Hour).Unix()
	for k, v := range p.m {
		if v.Seen < cut {
			delete(p.m, k)
		}
	}
	b, err := json.Marshal(p.m)
	p.dirty = false
	p.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}
