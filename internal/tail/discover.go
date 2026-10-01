package tail

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/p10node/p10logs/internal/cri"
)

// Filter decides which streams to collect.
type Filter struct {
	IncludeNS   []string
	ExcludeNS   []string
	ExcludeCtrs []string
	SelfNS      string
	SelfPod     string
	CollectSelf bool
}

func (f Filter) Allow(m cri.Meta) bool {
	if !f.CollectSelf && m.Namespace == f.SelfNS && m.Pod == f.SelfPod {
		return false
	}
	if len(f.IncludeNS) > 0 && !contains(f.IncludeNS, m.Namespace) {
		return false
	}
	if contains(f.ExcludeNS, m.Namespace) || contains(f.ExcludeCtrs, m.Container) {
		return false
	}
	return true
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// Discoverer polls the pods directory and keeps one Tailer per live (path, inode).
type Discoverer struct {
	Dir        string
	Positions  *Positions
	Sink       Sink
	Filter     Filter
	Backfill   bool
	MaxMerge   int
	Interval   time.Duration
	RotateWait time.Duration
	Log        *slog.Logger

	mu      sync.Mutex
	tailers map[string]*Tailer // by path
	wg      sync.WaitGroup
}

// Run blocks until ctx is done, then waits for tailers to stop.
func (d *Discoverer) Run(ctx context.Context) {
	if d.Interval == 0 {
		d.Interval = 2 * time.Second
	}
	d.tailers = map[string]*Tailer{}
	t := time.NewTicker(d.Interval)
	defer t.Stop()
	d.scan(ctx)
	for {
		select {
		case <-ctx.Done():
			d.wg.Wait()
			return
		case <-t.C:
			d.scan(ctx)
		}
	}
}

// Active returns the number of files being tailed.
func (d *Discoverer) Active() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.tailers)
}

// Lag returns the total bytes not yet read across tailed files.
func (d *Discoverer) Lag() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	var lag int64
	for p, t := range d.tailers {
		if st, err := os.Stat(p); err == nil {
			if l := st.Size() - t.Offset(); l > 0 {
				lag += l
			}
		}
	}
	return lag
}

func (d *Discoverer) scan(ctx context.Context) {
	files, _ := filepath.Glob(filepath.Join(d.Dir, "*", "*", "*.log"))
	seen := map[string]bool{}
	for _, p := range files {
		meta, ok := cri.ParsePath(p)
		if !ok || !d.Filter.Allow(meta) {
			continue
		}
		seen[p] = true
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		ino := Inode(st)
		d.mu.Lock()
		cur := d.tailers[p]
		d.mu.Unlock()
		if cur != nil {
			if cur.Inode == ino {
				continue
			}
			// rotated: same path, new inode. Let the old tailer drain, start a new one at 0.
			cur.Rotated()
			d.startTailer(ctx, p, ino, meta, 0)
			continue
		}
		// new file
		key := Key(p, ino)
		start := int64(0)
		if pos, ok := d.Positions.Get(key); ok {
			start = pos.Offset
		} else if d.Backfill {
			d.backfill(ctx, p, meta)
		}
		d.startTailer(ctx, p, ino, meta, start)
	}
	// tailers whose path vanished detect it themselves; drop finished ones from the map
	d.mu.Lock()
	for p, t := range d.tailers {
		select {
		case <-t.Done():
			delete(d.tailers, p)
		default:
			if !seen[p] {
				// file no longer listed (deleted or filtered): tailer will notice on its next stat
			}
		}
	}
	d.mu.Unlock()
}

func (d *Discoverer) startTailer(ctx context.Context, p string, ino uint64, meta cri.Meta, start int64) {
	t := NewTailer(p, ino, meta, start, d.Sink)
	t.MaxMerge = d.MaxMerge
	t.RotateWait = d.RotateWait
	d.mu.Lock()
	d.tailers[p] = t
	d.mu.Unlock()
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		t.Run(ctx)
		d.mu.Lock()
		if d.tailers[p] == t {
			delete(d.tailers, p)
		}
		d.mu.Unlock()
	}()
	if d.Log != nil {
		d.Log.Debug("tail", "path", p, "inode", ino, "start", start)
	}
}

// backfill reads rotated .gz files of a container the first time it is seen.
func (d *Discoverer) backfill(ctx context.Context, live string, meta cri.Meta) {
	dir := filepath.Dir(live)
	if d.Positions.HasAnyWithPrefix(dir + "/") {
		return // we have tailed this container before; rotated files were read live
	}
	gzs, _ := filepath.Glob(filepath.Join(dir, "*.log.*.gz"))
	sort.Strings(gzs) // name carries YYYYMMDD-HHMMSS → chronological
	for _, g := range gzs {
		if _, done := d.Positions.Get(g); done {
			continue
		}
		gm, ok := cri.ParsePath(g)
		if !ok {
			continue
		}
		if err := ReadGzip(ctx, g, gm, d.Sink, d.MaxMerge); err != nil && d.Log != nil {
			d.Log.Warn("backfill", "file", g, "err", err)
		}
		d.Positions.Set(g, Pos{Done: true})
	}
	// also the not-yet-gzipped rotated file, if any
	rot, _ := filepath.Glob(strings.TrimSuffix(live, ".log") + ".log.*")
	for _, r := range rot {
		if strings.HasSuffix(r, ".gz") {
			continue
		}
		rm, ok := cri.ParsePath(r)
		if !ok {
			continue
		}
		st, err := os.Stat(r)
		if err != nil {
			continue
		}
		t := NewTailer(r, Inode(st), rm, 0, d.Sink)
		t.MaxMerge = d.MaxMerge
		t.RotateWait = time.Millisecond
		t.Rotated() // read to EOF once and stop
		t.Run(ctx)
	}
}
