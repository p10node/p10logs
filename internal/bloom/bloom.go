// Package bloom is a fixed-size trigram bloom filter used to skip whole chunks
// during search. It is sound for the substring semantics of the query grammar:
// if `term` occurs as a substring of any token in the chunk, every trigram of the
// term's tokens is present, so a negative answer is never wrong.
package bloom

import (
	"encoding/base64"
	"errors"
)

const (
	mBits = 1 << 19 // 64 KiB per chunk
	k     = 4
)

// Filter is the bit set.
type Filter struct {
	bits []uint64
	n    int
}

// New allocates an empty filter.
func New() *Filter { return &Filter{bits: make([]uint64, mBits/64)} }

// Count is the number of trigram insertions (not unique).
func (f *Filter) Count() int { return f.n }

func isTok(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 0x80
}

func lower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

func h3(a, b, c byte) (uint32, uint32) {
	h := uint32(2166136261)
	h = (h ^ uint32(a)) * 16777619
	h = (h ^ uint32(b)) * 16777619
	h = (h ^ uint32(c)) * 16777619
	return h, h*0x9E3779B1 | 1
}

func (f *Filter) set(a, b, c byte) {
	h1, h2 := h3(a, b, c)
	for i := uint32(0); i < k; i++ {
		x := (h1 + i*h2) % mBits
		f.bits[x/64] |= 1 << (x % 64)
	}
	f.n++
}

func (f *Filter) has(a, b, c byte) bool {
	h1, h2 := h3(a, b, c)
	for i := uint32(0); i < k; i++ {
		x := (h1 + i*h2) % mBits
		if f.bits[x/64]&(1<<(x%64)) == 0 {
			return false
		}
	}
	return true
}

// Add inserts the trigrams of every token in msg.
func (f *Filter) Add(msg []byte) {
	var a, b byte
	run := 0
	for i := 0; i < len(msg); i++ {
		c := lower(msg[i])
		if !isTok(c) {
			run = 0
			continue
		}
		run++
		if run >= 3 {
			f.set(a, b, c)
		}
		a, b = b, c
	}
}

// MayContain reports whether term could occur as a substring of a token sequence.
// Terms with no token of length >= 3 always return true (cannot be decided).
func (f *Filter) MayContain(term string) bool {
	var a, b byte
	run := 0
	decided := false
	for i := 0; i < len(term); i++ {
		c := lower(term[i])
		if !isTok(c) {
			run = 0
			continue
		}
		run++
		if run >= 3 {
			decided = true
			if !f.has(a, b, c) {
				return false
			}
		}
		a, b = b, c
	}
	_ = decided
	return true
}

// Marshal encodes the bit set (base64 of little-endian words).
func (f *Filter) Marshal() string {
	raw := make([]byte, len(f.bits)*8)
	for i, w := range f.bits {
		for j := 0; j < 8; j++ {
			raw[i*8+j] = byte(w >> (8 * j))
		}
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// Unmarshal decodes a Marshal string.
func Unmarshal(s string) (*Filter, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(raw) != mBits/8 {
		return nil, errors.New("bloom: wrong size")
	}
	f := New()
	for i := range f.bits {
		var w uint64
		for j := 0; j < 8; j++ {
			w |= uint64(raw[i*8+j]) << (8 * j)
		}
		f.bits[i] = w
	}
	return f, nil
}

// Clone copies the filter (for a consistent snapshot of an open chunk).
func (f *Filter) Clone() *Filter {
	c := &Filter{bits: make([]uint64, len(f.bits)), n: f.n}
	copy(c.bits, f.bits)
	return c
}
