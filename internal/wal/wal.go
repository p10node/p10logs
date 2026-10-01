// Package wal is the hub's write-ahead log: every accepted push is appended and
// fsynced here before it is acknowledged, then buffered in memory and written to
// per-stream chunks in large frames. Records carry a global sequence number so
// replay can skip what already reached a chunk.
//
//	record = magic u32 | len u32 | crc32 u32 | seq u64 | metaLen u32 | meta JSON | raw entries
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/p10node/p10logs/internal/chunk"
)

const magic uint32 = 0x50315741 // "P1WA"

// Record is one accepted section.
type Record struct {
	Seq     uint64
	Meta    chunk.Meta
	Entries []chunk.Entry
}

// Log is an append-only, crash-safe log made of segment files.
type Log struct {
	dir  string
	mu   sync.Mutex
	f    *os.File
	size int64
	seq  uint64 // next sequence number
	seg  int
}

// Open opens the log, replaying existing segments through fn (in order) and
// truncating a corrupt tail. It returns the log positioned for appends.
func Open(dir string, fn func(Record) error) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	sort.Strings(segs)
	l := &Log{dir: dir, seq: 1}
	for i, p := range segs {
		last := i == len(segs)-1
		n, size, err := replay(p, fn, func(seq uint64) {
			if seq >= l.seq {
				l.seq = seq + 1
			}
		})
		if err != nil {
			return nil, err
		}
		if last {
			f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return nil, err
			}
			if n == 0 && size > 0 { // wholly corrupt: start fresh
				f.Truncate(0)
				size = 0
			}
			l.f, l.size = f, size
			fmt.Sscanf(filepath.Base(p), "%06d.wal", &l.seg)
		}
	}
	if l.f == nil {
		if err := l.rotate(); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func replay(p string, fn func(Record) error, seen func(uint64)) (records int, validSize int64, err error) {
	f, err := os.Open(p)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	st, _ := f.Stat()
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	for {
		var hdr [12]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break
		}
		if binary.LittleEndian.Uint32(hdr[0:4]) != magic {
			break
		}
		n := int64(binary.LittleEndian.Uint32(hdr[4:8]))
		crc := binary.LittleEndian.Uint32(hdr[8:12])
		if n < 12 || off+12+n > st.Size() {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		if crc32.ChecksumIEEE(body) != crc {
			break
		}
		rec, err := decode(body)
		if err != nil {
			break
		}
		seen(rec.Seq)
		if err := fn(rec); err != nil {
			return records, off, err
		}
		records++
		off += 12 + n
	}
	if off != st.Size() {
		f.Close()
		if err := os.Truncate(p, off); err != nil {
			return records, off, err
		}
	}
	return records, off, nil
}

func decode(body []byte) (Record, error) {
	if len(body) < 12 {
		return Record{}, errors.New("wal: short record")
	}
	var rec Record
	rec.Seq = binary.LittleEndian.Uint64(body[0:8])
	ml := int(binary.LittleEndian.Uint32(body[8:12]))
	if 12+ml > len(body) {
		return Record{}, errors.New("wal: bad meta length")
	}
	if err := json.Unmarshal(body[12:12+ml], &rec.Meta); err != nil {
		return Record{}, err
	}
	raw := body[12+ml:]
	if err := chunk.DecodeRaw(raw, func(e chunk.Entry) bool {
		rec.Entries = append(rec.Entries, chunk.Entry{TS: e.TS, Stderr: e.Stderr, Msg: append([]byte(nil), e.Msg...)})
		return true
	}); err != nil {
		return Record{}, err
	}
	return rec, nil
}

func (l *Log) rotate() error {
	if l.f != nil {
		l.f.Close()
	}
	l.seg++
	p := filepath.Join(l.dir, fmt.Sprintf("%06d.wal", l.seg))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	l.f, l.size = f, 0
	return nil
}

// Append writes and fsyncs one record, returning its sequence number.
func (l *Log) Append(meta chunk.Meta, entries []chunk.Entry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.seq
	mb, _ := json.Marshal(meta)
	raw := chunk.EncodeRaw(entries)
	body := make([]byte, 0, 12+len(mb)+len(raw))
	var tmp [12]byte
	binary.LittleEndian.PutUint64(tmp[0:8], seq)
	binary.LittleEndian.PutUint32(tmp[8:12], uint32(len(mb)))
	body = append(body, tmp[:]...)
	body = append(body, mb...)
	body = append(body, raw...)
	var hdr [12]byte
	binary.LittleEndian.PutUint32(hdr[0:4], magic)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[8:12], crc32.ChecksumIEEE(body))
	if _, err := l.f.Write(append(hdr[:], body...)); err != nil {
		return 0, err
	}
	if err := l.f.Sync(); err != nil {
		return 0, err
	}
	l.size += int64(12 + len(body))
	l.seq++
	return seq, nil
}

// Size is the current segment size.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// TotalSize sums all segments on disk.
func (l *Log) TotalSize() int64 {
	segs, _ := filepath.Glob(filepath.Join(l.dir, "*.wal"))
	var n int64
	for _, s := range segs {
		if st, err := os.Stat(s); err == nil {
			n += st.Size()
		}
	}
	return n
}

// Checkpoint starts a new segment and deletes all older ones. The caller must
// have flushed every buffered record to chunks first.
func (l *Log) Checkpoint() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	old, _ := filepath.Glob(filepath.Join(l.dir, "*.wal"))
	if err := l.rotate(); err != nil {
		return err
	}
	cur := filepath.Base(l.f.Name())
	for _, p := range old {
		if !strings.HasSuffix(p, cur) {
			os.Remove(p)
		}
	}
	return nil
}

// Close closes the current segment.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
