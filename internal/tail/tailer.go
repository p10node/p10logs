package tail

import (
	"bufio"
	"bytes"
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

// Tailer follows one file identity (path + inode + first line) to EOF, then keeps polling.
type Tailer struct {
	Path  string
	Inode uint64
	Meta  cri.Meta
	// Start is the offset to resume from when Positions is nil; with Positions set, the
	// checkpoint for the file's FileKey (or, once, its legacy Key) decides.
	Start     int64
	Positions *Positions
	Sink      Sink
	Poll      time.Duration
	MaxMerge  int
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

// NewTailer prepares a tailer; call Run in a goroutine. The file key is fixed once the
// first line is on disk (see FileKey).
func NewTailer(path string, inode uint64, meta cri.Meta, start int64, sink Sink) *Tailer {
	return &Tailer{Path: path, Inode: inode, Meta: meta, Start: start, Sink: sink, Poll: 250 * time.Millisecond,
		rotated: make(chan struct{}), done: make(chan struct{})}
}

// Key returns the file key ("" until the first line has been seen).
func (t *Tailer) Key() string { return t.key }

// fingerprintMax bounds how much of the file the identity hash covers. CRI records can be
// 16 KiB (containerd) plus prefix before the first newline, so 4 KiB is not enough; a
// file that has no newline within 64 KiB is fingerprinted by those 64 KiB.
const fingerprintMax = 64 << 10

// firstLine returns the bytes that identify the file: its first line, or the first
// fingerprintMax bytes when the first line is longer than that. ok is false while the
// file has no complete line yet (a log reopened by the runtime is empty for a moment).
func firstLine(f *os.File) (line []byte, ok bool) {
	buf := make([]byte, fingerprintMax)
	n, _ := f.ReadAt(buf, 0)
	if i := bytes.IndexByte(buf[:n], '\n'); i >= 0 {
		return buf[:i], true
	}
	if n == fingerprintMax {
		return buf, true
	}
	return nil, false
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
	// Identity needs the first line; a freshly reopened log may be empty for a moment.
	var head []byte
	for {
		var ok bool
		if head, ok = firstLine(f); ok {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(t.Poll):
		}
		if st, err := os.Stat(t.Path); err != nil || Inode(st) != Inode(fi) {
			if fi2, err := f.Stat(); err != nil || fi2.Size() == 0 {
				return // replaced or deleted while still empty: nothing to ship
			}
		}
	}
	t.key = FileKey(t.Path, Inode(fi), head)
	off := t.Start
	if t.Positions != nil {
		if p, ok := t.Positions.Get(t.key); ok {
			off = p.Offset
		} else if p, ok := t.Positions.Get(Key(t.Path, Inode(fi))); ok {
			off = p.Offset // checkpoint written by an agent before FileKey existed: adopt it once
		} else {
			off = 0
		}
	}
	if fi, err = f.Stat(); err != nil {
		return
	}
	if fi.Size() < off { // truncated since checkpoint
		off = 0
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return
	}
	r := bufio.NewReaderSize(f, 64<<10)
	merger := cri.Merger{MaxSize: t.MaxMerge}
	var pending []byte
	idle, missing := 0, 0
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
		if idle%8 == 0 { // every ~2s check for deletion / replacement / truncation
			st, err := os.Stat(t.Path)
			if err != nil {
				// Path gone. Between kubelet's rename and the runtime's reopen the path is
				// briefly absent, so end the stream only when it is still gone ~1 s later.
				missing++
				if missing < 2 {
					idle = 4 // look again after 4 polls (~1 s) instead of the usual 8
					continue
				}
				if len(pending) > 0 {
					off += int64(len(pending))
					t.emit(&merger, pending, off)
					pending = nil
				}
				if l, ok := merger.Flush(); ok {
					t.Sink.Record(Record{Meta: t.Meta, Key: t.key, Time: l.Time, Stderr: l.Stderr, Msg: l.Msg, StartOff: t.mergeAt, EndOff: off})
				}
				t.Sink.StreamEnd(t.Meta, t.key)
				return
			}
			missing = 0
			if Inode(st) != Inode(fi) {
				// Replaced under us (rotation noticed here before the discoverer said so).
				// Keep draining this inode for the grace period: the runtime may still write
				// to it until it reopens the new path. Returning here lost those lines.
				if rotatedAt.IsZero() {
					rotatedAt = time.Now()
				}
				continue
			}
			if st.Size() < off { // truncated in place (CRI-O log_size_max, cri-dockerd): new content, new identity
				if l, ok := merger.Flush(); ok {
					t.Sink.Record(Record{Meta: t.Meta, Key: t.key, Time: l.Time, Stderr: l.Stderr, Msg: l.Msg, StartOff: t.mergeAt, EndOff: off})
				}
				return // the discoverer starts a fresh tailer, keyed by the new first line
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
