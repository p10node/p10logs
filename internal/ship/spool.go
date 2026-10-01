package ship

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

// Spool is a bounded on-disk queue of batch bodies, used while the hub is unreachable.
type Spool struct {
	Dir      string
	MaxBytes int64
	seq      int64
	Dropped  atomic.Int64
	size     atomic.Int64
}

// Open scans an existing spool directory.
func (s *Spool) Open() error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	files, _ := s.list()
	var size int64
	for _, f := range files {
		if st, err := os.Stat(f); err == nil {
			size += st.Size()
		}
	}
	s.size.Store(size)
	s.seq = time.Now().UnixNano()
	return nil
}

func (s *Spool) list() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(s.Dir, "*.zst"))
	sort.Strings(files)
	return files, err
}

// Size is the current spool size in bytes.
func (s *Spool) Size() int64 { return s.size.Load() }

// Empty reports whether nothing is queued.
func (s *Spool) Empty() bool {
	files, _ := s.list()
	return len(files) == 0
}

// Put appends a body, evicting the oldest files when over MaxBytes.
func (s *Spool) Put(body []byte) error {
	s.seq++
	p := filepath.Join(s.Dir, fmt.Sprintf("%020d.zst", s.seq))
	if err := os.WriteFile(p+".tmp", body, 0o644); err != nil {
		return err
	}
	if err := os.Rename(p+".tmp", p); err != nil {
		return err
	}
	s.size.Add(int64(len(body)))
	for s.MaxBytes > 0 && s.size.Load() > s.MaxBytes {
		files, _ := s.list()
		if len(files) <= 1 {
			break
		}
		if st, err := os.Stat(files[0]); err == nil {
			s.size.Add(-st.Size())
		}
		os.Remove(files[0])
		s.Dropped.Add(1)
	}
	return nil
}

// Drain sends queued bodies oldest-first; stops at the first failure.
func (s *Spool) Drain(ctx context.Context, send func(context.Context, []byte) error, log *slog.Logger) error {
	files, err := s.list()
	if err != nil {
		return err
	}
	for _, f := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		body, err := os.ReadFile(f)
		if err != nil {
			os.Remove(f)
			continue
		}
		if err := send(ctx, body); err != nil {
			if errors.Is(err, ErrTooLarge) {
				log.Warn("spool: dropping oversized batch", "file", f)
				os.Remove(f)
				s.size.Add(-int64(len(body)))
				continue
			}
			return err
		}
		os.Remove(f)
		s.size.Add(-int64(len(body)))
	}
	return nil
}
