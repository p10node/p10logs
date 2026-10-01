package tail

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/p10node/p10logs/internal/cri"
)

// Record is one complete log line handed to the sink, with the byte offset just
// after it in the source file (the checkpoint to store once it is acknowledged).
type Record struct {
	Meta     cri.Meta
	Key      string // positions key (path@inode) or gz file name
	Time     time.Time
	Stderr   bool
	Msg      []byte // valid only during Sink call
	StartOff int64  // byte offset of the first raw line of this record
	EndOff   int64  // byte offset just after the last raw line of this record
}

// Sink receives records; it may block to apply backpressure.
type Sink interface {
	Record(r Record)
	// StreamEnd is called when the file has been removed (pod deleted / container GC'd).
	StreamEnd(meta cri.Meta, key string)
}

// Inode returns the inode of an open file (0 on non-unix).
func Inode(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}

// Tailer follows one file identity (path + inode) to EOF, then keeps polling.
type Tailer struct {
	Path     string
	Inode    uint64
	Meta     cri.Meta
	Start    int64
	Sink     Sink
	Poll     time.Duration
	MaxMerge int
	// RotateWait: after rotation keep reading the old inode this long (runtime may
	// still flush into it before it reopens the new path). Default 5s.
	RotateWait time.Duration
	// rotated is set when the path now points at a different inode: drain and stop.
	rotated chan struct{}
	done    chan struct{}
	key     string
	off     atomic.Int64
	mergeAt int64
}

// Offset is the current read position (for lag reporting).
func (t *Tailer) Offset() int64 { return t.off.Load() }

// NewTailer prepares a tailer; call Run in a goroutine.
func NewTailer(path string, inode uint64, meta cri.Meta, start int64, sink Sink) *Tailer {
	return &Tailer{Path: path, Inode: inode, Meta: meta, Start: start, Sink: sink, Poll: 250 * time.Millisecond,
		rotated: make(chan struct{}), done: make(chan struct{}), key: Key(path, inode)}
}

// Rotated tells the tailer its file was rotated away: finish the old inode and exit.
func (t *Tailer) Rotated() {
	select {
	case <-t.rotated:
	default:
		close(t.rotated)
	}
}

// Done is closed when Run returns.
func (t *Tailer) Done() <-chan struct{} { return t.done }

// Run reads until ctx is cancelled, the file disappears, or Rotated() and EOF.
func (t *Tailer) Run(ctx context.Context) {
	defer close(t.done)
	f, err := os.Open(t.Path)
	if err != nil {
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || (t.Inode != 0 && Inode(fi) != t.Inode) {
		return
	}
	off := t.Start
	if fi.Size() < off { // truncated since checkpoint
		off = 0
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return
	}
	r := bufio.NewReaderSize(f, 64<<10)
	merger := cri.Merger{MaxSize: t.MaxMerge}
	var pending []byte
	idle := 0
	var rotatedAt time.Time
	rw := t.RotateWait
	if rw == 0 {
		rw = 5 * time.Second
	}
	for {
		line, err := r.ReadSlice('\n')
		if len(line) > 0 && err == nil {
			// complete line
			var full []byte
			if pending != nil {
				pending = append(pending, line...)
				full = pending
				pending = nil
			} else {
				full = line
			}
			off += int64(len(full))
			t.emit(&merger, full, off)
			idle = 0
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			pending = append(pending, line...)
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}
		// EOF (line may hold a partial line without newline: keep it for the next read)
		if len(line) > 0 {
			pending = append(pending, line...)
		}
		if !rotatedAt.IsZero() && time.Since(rotatedAt) > rw {
			// rotated and the grace period is over: flush and stop
			if len(pending) > 0 {
				off += int64(len(pending))
				t.emit(&merger, pending, off)
				pending = nil
			}
			if l, ok := merger.Flush(); ok {
				t.Sink.Record(Record{Meta: t.Meta, Key: t.key, Time: l.Time, Stderr: l.Stderr, Msg: l.Msg, StartOff: t.mergeAt, EndOff: off})
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.rotated:
			if rotatedAt.IsZero() {
				rotatedAt = time.Now()
			}
			t.rotated = nil // already signalled; keep polling until the grace period ends
		case <-time.After(t.Poll):
		}
		idle++
		if idle%8 == 0 { // every ~2s check for deletion / truncation
			st, err := os.Stat(t.Path)
			if err != nil || Inode(st) != Inode(fi) {
				// file gone or replaced under us; if it is really gone the pod was deleted
				if l, ok := merger.Flush(); ok {
					t.Sink.Record(Record{Meta: t.Meta, Key: t.key, Time: l.Time, Stderr: l.Stderr, Msg: l.Msg, StartOff: t.mergeAt, EndOff: off})
				}
				if err != nil {
					t.Sink.StreamEnd(t.Meta, t.key)
				}
				return
			}
			if st.Size() < off { // truncated in place (CRI-O log_size_max, cri-dockerd)
				off = 0
				f.Seek(0, io.SeekStart)
				r.Reset(f)
				pending = nil
			}
		}
	}
}

func (t *Tailer) emit(m *cri.Merger, raw []byte, endOff int64) {
	t.off.Store(endOff)
	start := endOff - int64(len(raw))
	l, err := cri.Parse(raw)
	if err != nil {
		return // malformed line (e.g. mid-write garbage after truncation): skip
	}
	if !m.Open() {
		t.mergeAt = start
	}
	out, ok := m.Push(l)
	if !ok {
		return
	}
	t.Sink.Record(Record{Meta: t.Meta, Key: t.key, Time: out.Time, Stderr: out.Stderr, Msg: out.Msg, StartOff: t.mergeAt, EndOff: endOff})
}

// ReadGzip streams a rotated .gz file (backfill) through the sink; EndOff is the
// uncompressed offset. Key is the file path (no inode; gz files are immutable).
func ReadGzip(ctx context.Context, path string, meta cri.Meta, sink Sink, maxMerge int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	r := bufio.NewReaderSize(gz, 64<<10)
	m := cri.Merger{MaxSize: maxMerge}
	var off, mergeAt int64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			start := off
			off += int64(len(line))
			if l, perr := cri.Parse(line); perr == nil {
				if !m.Open() {
					mergeAt = start
				}
				if out, ok := m.Push(l); ok {
					sink.Record(Record{Meta: meta, Key: path, Time: out.Time, Stderr: out.Stderr, Msg: out.Msg, StartOff: mergeAt, EndOff: off})
				}
			}
		}
		if err != nil {
			if l, ok := m.Flush(); ok {
				sink.Record(Record{Meta: meta, Key: path, Time: l.Time, Stderr: l.Stderr, Msg: l.Msg, StartOff: mergeAt, EndOff: off})
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
