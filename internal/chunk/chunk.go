// Package chunk implements the hub's on-disk chunk format: an append-only file of
// independent zstd frames, one per received batch, plus a footer written at seal time.
//
//	frame  = magic u32 | metaLen u32 | payLen u32 | crc32 u32 | meta JSON | zstd(payload)
//	payload = repeated { ts int64 | stream u8 | len u32 | msg }
//	footer = footer JSON | footerLen u32 | fmagic u32          (sealed chunks only)
package chunk

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/p10node/p10logs/internal/bloom"
)

const (
	Magic  uint32 = 0x50314c46 // "P1LF"
	FMagic uint32 = 0x50314c54 // "P1LT"
	hdrLen        = 16
)

// Meta is stored in every frame header; it carries the agent cursor so the index
// can always be re-derived from the data files.
type Meta struct {
	Cluster string            `json:"cl,omitempty"` // stream identity, so the index is always derivable
	Key     string            `json:"k,omitempty"`  // ns/pod/uid/ctr
	File    string            `json:"f"`
	Off     [2]int64          `json:"o"`
	Node    string            `json:"n,omitempty"`
	Restart int               `json:"r"`
	End     bool              `json:"end,omitempty"`
	Labels  map[string]string `json:"l,omitempty"`
	WSeq    uint64            `json:"w,omitempty"` // max WAL sequence covered (replay skips <=)
	MinTS   int64             `json:"min"`
	MaxTS   int64             `json:"max"`
	Count   int               `json:"c"`
}

// FrameRef locates one frame inside a chunk.
type FrameRef struct {
	Off   int64 `json:"off"`
	Len   int64 `json:"len"`
	MinTS int64 `json:"min"`
	MaxTS int64 `json:"max"`
	Count int   `json:"c"`
}

// Footer summarises a sealed chunk.
type Footer struct {
	Version int               `json:"v"`
	Cluster string            `json:"cl,omitempty"`
	Key     string            `json:"k,omitempty"`
	Restart int               `json:"r,omitempty"`
	Labels  map[string]string `json:"l,omitempty"`
	WSeq    uint64            `json:"w,omitempty"`
	File    string            `json:"f,omitempty"` // last frame's file id + end offset = cursor
	Off     int64             `json:"o,omitempty"`
	MinTS   int64             `json:"min"`
	MaxTS   int64             `json:"max"`
	Lines   int               `json:"lines"`
	Bytes   int64             `json:"bytes"` // uncompressed payload bytes
	Frames  []FrameRef        `json:"frames"`
	Bloom   string            `json:"bloom,omitempty"` // trigram bloom over all lines (bloom.Marshal)
}

// Entry is one decoded line.
type Entry struct {
	TS     int64
	Stderr bool
	Msg    []byte
}

var (
	encPool = sync.Pool{New: func() any {
		e, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		return e
	}}
	decPool = sync.Pool{New: func() any { d, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1)); return d }}
)

// EncodeRaw serialises entries (uncompressed): repeated { ts i64 | stream u8 | len u32 | msg }.
func EncodeRaw(entries []Entry) []byte {
	n := 0
	for _, e := range entries {
		n += 13 + len(e.Msg)
	}
	buf := make([]byte, 0, n)
	var tmp [13]byte
	for _, e := range entries {
		binary.LittleEndian.PutUint64(tmp[:8], uint64(e.TS))
		if e.Stderr {
			tmp[8] = 1
		} else {
			tmp[8] = 0
		}
		binary.LittleEndian.PutUint32(tmp[9:13], uint32(len(e.Msg)))
		buf = append(buf, tmp[:]...)
		buf = append(buf, e.Msg...)
	}
	return buf
}

// DecodeRaw reverses EncodeRaw. Msg slices alias raw.
func DecodeRaw(raw []byte, fn func(Entry) bool) error {
	for len(raw) >= 13 {
		ts := int64(binary.LittleEndian.Uint64(raw[:8]))
		st := raw[8] == 1
		n := int(binary.LittleEndian.Uint32(raw[9:13]))
		raw = raw[13:]
		if n > len(raw) {
			return errors.New("chunk: truncated payload")
		}
		if !fn(Entry{TS: ts, Stderr: st, Msg: raw[:n]}) {
			return nil
		}
		raw = raw[n:]
	}
	return nil
}

// EncodePayload serialises entries and compresses them.
func EncodePayload(entries []Entry) (compressed []byte, raw int) {
	buf := EncodeRaw(entries)
	enc := encPool.Get().(*zstd.Encoder)
	out := enc.EncodeAll(buf, nil)
	encPool.Put(enc)
	return out, len(buf)
}

// DecodePayload decompresses and decodes a frame payload.
func DecodePayload(compressed []byte, fn func(Entry) bool) error {
	dec := decPool.Get().(*zstd.Decoder)
	raw, err := dec.DecodeAll(compressed, nil)
	decPool.Put(dec)
	if err != nil {
		return err
	}
	return DecodeRaw(raw, fn)
}

// Writer appends frames to an open chunk.
type Writer struct {
	f       *os.File
	size    int64
	Cluster string
	Key     string
	Restart int
	Labels  map[string]string
	WSeq    uint64
	Frames  []FrameRef
	last    *Meta
	Bloom   *bloom.Filter
	Lines   int
	Bytes   int64
	MinTS   int64
	MaxTS   int64
}

// Create opens (or creates) an .open chunk for appending, recovering any valid frames.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f, Bloom: bloom.New()}
	if err := w.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// recover scans existing frames, truncating at the first corrupt one.
func (w *Writer) recover() error {
	st, err := w.f.Stat()
	if err != nil {
		return err
	}
	var off int64
	r := bufio.NewReaderSize(io.NewSectionReader(w.f, 0, st.Size()), 1<<16)
	for {
		ref, meta, payload, err := readFrame(r, off, st.Size())
		if err != nil {
			break
		}
		w.addRef(ref, meta)
		DecodePayload(payload, func(e Entry) bool { w.Bloom.Add(e.Msg); return true })
		off += ref.Len
	}
	if off != st.Size() {
		if err := w.f.Truncate(off); err != nil {
			return err
		}
	}
	w.size = off
	_, err = w.f.Seek(off, io.SeekStart)
	return err
}

func (w *Writer) addRef(ref FrameRef, meta *Meta) {
	if w.Lines == 0 || ref.MinTS < w.MinTS {
		w.MinTS = ref.MinTS
	}
	if ref.MaxTS > w.MaxTS {
		w.MaxTS = ref.MaxTS
	}
	w.Lines += ref.Count
	w.Frames = append(w.Frames, ref)
	if meta != nil {
		w.last = meta
		if meta.Key != "" {
			w.Cluster, w.Key = meta.Cluster, meta.Key
		}
		if meta.Restart > w.Restart {
			w.Restart = meta.Restart
		}
		if len(meta.Labels) > 0 {
			w.Labels = meta.Labels
		}
		if meta.WSeq > w.WSeq {
			w.WSeq = meta.WSeq
		}
	}
}

// readFrame reads and validates one frame at off, returning its compressed payload.
// On success the reader is positioned at the next frame.
func readFrame(r *bufio.Reader, off, limit int64) (FrameRef, *Meta, []byte, error) {
	var hdr [hdrLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return FrameRef{}, nil, nil, err
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != Magic {
		return FrameRef{}, nil, nil, errors.New("bad magic")
	}
	ml := int64(binary.LittleEndian.Uint32(hdr[4:8]))
	pl := int64(binary.LittleEndian.Uint32(hdr[8:12]))
	crc := binary.LittleEndian.Uint32(hdr[12:16])
	if ml > 1<<20 || pl > 64<<20 || off+hdrLen+ml+pl > limit {
		return FrameRef{}, nil, nil, errors.New("frame exceeds file")
	}
	body := make([]byte, ml+pl)
	if _, err := io.ReadFull(r, body); err != nil {
		return FrameRef{}, nil, nil, err
	}
	if crc32.ChecksumIEEE(body) != crc {
		return FrameRef{}, nil, nil, errors.New("crc mismatch")
	}
	var m Meta
	if err := json.Unmarshal(body[:ml], &m); err != nil {
		return FrameRef{}, nil, nil, err
	}
	return FrameRef{Off: off, Len: hdrLen + ml + pl, MinTS: m.MinTS, MaxTS: m.MaxTS, Count: m.Count}, &m, body[ml:], nil
}

// Append writes one frame and fsyncs.
func (w *Writer) Append(meta Meta, entries []Entry) (FrameRef, error) {
	for _, e := range entries {
		w.Bloom.Add(e.Msg)
	}
	payload, raw := EncodePayload(entries)
	mb, _ := json.Marshal(meta)
	var hdr [hdrLen]byte
	binary.LittleEndian.PutUint32(hdr[0:4], Magic)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(mb)))
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(payload)))
	h := crc32.NewIEEE()
	h.Write(mb)
	h.Write(payload)
	binary.LittleEndian.PutUint32(hdr[12:16], h.Sum32())
	buf := make([]byte, 0, hdrLen+len(mb)+len(payload))
	buf = append(buf, hdr[:]...)
	buf = append(buf, mb...)
	buf = append(buf, payload...)
	if _, err := w.f.Write(buf); err != nil {
		return FrameRef{}, err
	}
	if err := w.f.Sync(); err != nil {
		return FrameRef{}, err
	}
	ref := FrameRef{Off: w.size, Len: int64(len(buf)), MinTS: meta.MinTS, MaxTS: meta.MaxTS, Count: meta.Count}
	w.size += ref.Len
	w.Bytes += int64(raw)
	w.addRef(ref, &meta)
	return ref, nil
}

// Size is the current file size.
func (w *Writer) Size() int64 { return w.size }

// LastMeta returns the meta of the last frame (cursor recovery), or nil.
func (w *Writer) LastMeta() *Meta { return w.last }

// Seal writes the footer, closes the file and renames it to sealedPath.
func (w *Writer) Seal(sealedPath string) (Footer, error) {
	ft := Footer{Version: 1, Cluster: w.Cluster, Key: w.Key, Restart: w.Restart, Labels: w.Labels, WSeq: w.WSeq, MinTS: w.MinTS, MaxTS: w.MaxTS, Lines: w.Lines, Bytes: w.Bytes, Frames: w.Frames}
	if w.last != nil {
		ft.File, ft.Off = w.last.File, w.last.Off[1]
	}
	if w.Bloom != nil && w.Bloom.Count() > 0 {
		ft.Bloom = w.Bloom.Marshal()
	}
	fb, _ := json.Marshal(ft)
	var tail [8]byte
	binary.LittleEndian.PutUint32(tail[0:4], uint32(len(fb)))
	binary.LittleEndian.PutUint32(tail[4:8], FMagic)
	if _, err := w.f.Write(append(fb, tail[:]...)); err != nil {
		return ft, err
	}
	if err := w.f.Sync(); err != nil {
		return ft, err
	}
	name := w.f.Name()
	if err := w.f.Close(); err != nil {
		return ft, err
	}
	return ft, os.Rename(name, sealedPath)
}

// Close closes without sealing (chunk stays .open and is recovered on restart).
func (w *Writer) Close() error { return w.f.Close() }

// ReadFooter reads a sealed chunk's footer.
func ReadFooter(path string) (Footer, error) {
	f, err := os.Open(path)
	if err != nil {
		return Footer{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return Footer{}, err
	}
	if st.Size() < 8 {
		return Footer{}, errors.New("chunk: too small")
	}
	var tail [8]byte
	if _, err := f.ReadAt(tail[:], st.Size()-8); err != nil {
		return Footer{}, err
	}
	if binary.LittleEndian.Uint32(tail[4:8]) != FMagic {
		return Footer{}, errors.New("chunk: no footer")
	}
	fl := int64(binary.LittleEndian.Uint32(tail[0:4]))
	if fl > st.Size()-8 {
		return Footer{}, errors.New("chunk: bad footer length")
	}
	fb := make([]byte, fl)
	if _, err := f.ReadAt(fb, st.Size()-8-fl); err != nil {
		return Footer{}, err
	}
	var ft Footer
	return ft, json.Unmarshal(fb, &ft)
}

// DecodeFooterTail parses a footer given the file's last (8 + footerLen) bytes, or
// just the last 8 bytes to learn footerLen (returned as need > 0 when more is needed).
func DecodeFooterTail(tail []byte) (ft Footer, need int, err error) {
	if len(tail) < 8 {
		return ft, 8, nil
	}
	end := tail[len(tail)-8:]
	if binary.LittleEndian.Uint32(end[4:8]) != FMagic {
		return ft, 0, errors.New("chunk: no footer")
	}
	fl := int(binary.LittleEndian.Uint32(end[0:4]))
	if len(tail) < 8+fl {
		return ft, 8 + fl, nil
	}
	fb := tail[len(tail)-8-fl : len(tail)-8]
	return ft, 0, json.Unmarshal(fb, &ft)
}

// ReadFrame decodes the frame at ref from path, calling fn per entry (return false to stop).
func ReadFrame(f *os.File, ref FrameRef, fn func(Entry) bool) (*Meta, error) {
	buf := make([]byte, ref.Len)
	if _, err := f.ReadAt(buf, ref.Off); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != Magic {
		return nil, errors.New("chunk: bad magic")
	}
	ml := binary.LittleEndian.Uint32(buf[4:8])
	pl := binary.LittleEndian.Uint32(buf[8:12])
	if int64(hdrLen+ml+pl) != ref.Len {
		return nil, errors.New("chunk: frame length mismatch")
	}
	var m Meta
	if err := json.Unmarshal(buf[hdrLen:hdrLen+ml], &m); err != nil {
		return nil, err
	}
	return &m, DecodePayload(buf[hdrLen+ml:], fn)
}

// ScanOpen lists frames of an .open chunk (no footer) without a Writer; used by readers.
func ScanOpen(path string) ([]FrameRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, _ := f.Stat()
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, st.Size()), 1<<16)
	var refs []FrameRef
	var off int64
	for {
		ref, _, _, err := readFrame(r, off, st.Size())
		if err != nil {
			break
		}
		refs = append(refs, ref)
		off += ref.Len
	}
	return refs, nil
}
