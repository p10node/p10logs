// Package ship turns tail records into zstd NDJSON batches and delivers them to the hub
// with at-least-once semantics (disk spool while the hub is unreachable).
package ship

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/p10node/p10logs/internal/cri"
	"github.com/p10node/p10logs/internal/tail"
	"github.com/p10node/p10logs/internal/wire"
)

// Ack is a checkpoint to persist once the batch it belongs to is durable.
type Ack struct {
	Key    string
	EndOff int64
}

// Batch is one push body plus the checkpoints it carries.
type Batch struct {
	Body  []byte // zstd(NDJSON)
	Acks  []Ack
	Lines int
	Raw   int
}

// BatcherConfig mirrors the agent config.
type BatcherConfig struct {
	MaxBytes      int
	MaxLines      int
	FlushInterval time.Duration
	LinesPerSec   float64 // per container, 0 = unlimited
	Multiline     bool
	StartPattern  string
	MultiMaxLines int
	MultiTimeout  time.Duration // flush a multiline group this long after its last line (default 2s)
	MaxLineBytes  int
}

type entry struct {
	t      int64
	stderr bool
	msg    []byte
}

type section struct {
	meta    cri.Meta
	file    string
	start   int64
	end     int64
	entries []entry
	// multiline pending
	pend    *entry
	pendN   int
	pendEnd int64
	pendAt  time.Time // when the last line joined the group
	// rate limit
	tokens  float64
	last    time.Time
	dropped int
	ended   bool
}

// Batcher groups records per stream and emits batches on a channel.
type Batcher struct {
	cfg     BatcherConfig
	out     chan *Batch
	mu      sync.Mutex
	secs    map[string]*section // by positions key (file identity)
	order   []string
	bytes   int
	lines   int
	re      *regexp.Regexp
	enc     *zstd.Encoder
	Dropped int64 // rate-limited lines, for stats
	// LabelsFor returns enrichment labels for a pod uid (nil = none). Optional.
	LabelsFor func(uid string) map[string]string
}

// NewBatcher creates a batcher; out receives finished batches (bounded → backpressure).
func NewBatcher(cfg BatcherConfig, inflight int) *Batcher {
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1 << 20
	}
	if cfg.MaxLines == 0 {
		cfg.MaxLines = 5000
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.MaxLineBytes == 0 {
		cfg.MaxLineBytes = 256 << 10
	}
	if cfg.MultiTimeout <= 0 {
		cfg.MultiTimeout = 2 * time.Second
	}
	if inflight <= 0 {
		inflight = 4
	}
	b := &Batcher{cfg: cfg, out: make(chan *Batch, inflight), secs: map[string]*section{}}
	if cfg.Multiline && cfg.StartPattern != "" {
		b.re, _ = regexp.Compile(cfg.StartPattern)
	}
	b.enc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	return b
}

// Out is the channel of finished batches.
func (b *Batcher) Out() <-chan *Batch { return b.out }

// Run flushes on the interval until ctx is done, then does a final flush and closes Out.
func (b *Batcher) Run(ctx context.Context) {
	t := time.NewTicker(b.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			b.flush(true, true)
			close(b.out)
			return
		case <-t.C:
			b.flush(true, false)
		}
	}
}

// Record implements tail.Sink.
func (b *Batcher) Record(r tail.Record) {
	b.mu.Lock()
	s := b.secs[r.Key]
	if s == nil {
		s = &section{meta: r.Meta, file: r.Key, start: r.StartOff, tokens: b.cfg.LinesPerSec, last: time.Now()}
		b.secs[r.Key] = s
		b.order = append(b.order, r.Key)
	}
	// rate limit (token bucket per container file)
	if b.cfg.LinesPerSec > 0 {
		now := time.Now()
		s.tokens += now.Sub(s.last).Seconds() * b.cfg.LinesPerSec
		if s.tokens > b.cfg.LinesPerSec {
			s.tokens = b.cfg.LinesPerSec
		}
		s.last = now
		if s.tokens < 1 {
			s.dropped++
			b.Dropped++
			s.end = r.EndOff
			b.mu.Unlock()
			return
		}
		s.tokens--
		if s.dropped > 0 {
			b.add(s, entry{t: r.Time.UnixNano(), stderr: true, msg: []byte("p10logs: dropped " + itoa(s.dropped) + " lines (rate limit)")}, r.EndOff)
			s.dropped = 0
		}
	}
	msg := r.Msg
	if len(msg) > b.cfg.MaxLineBytes {
		msg = append(append([]byte{}, msg[:b.cfg.MaxLineBytes]...), []byte("…[truncated]")...)
	}
	e := entry{t: r.Time.UnixNano(), stderr: r.Stderr, msg: append([]byte(nil), msg...)}
	if b.re != nil {
		if s.pend != nil && !b.re.Match(e.msg) && s.pendN < b.cfg.MultiMaxLines {
			s.pend.msg = append(append(s.pend.msg, '\n'), e.msg...)
			s.pendN++
			s.pendEnd = r.EndOff
			s.pendAt = time.Now()
			b.mu.Unlock()
			return
		}
		if s.pend != nil { // a new start line closes the previous group
			b.add(s, *s.pend, s.pendEnd)
		}
		s.pend = &e
		s.pendN = 1
		s.pendEnd = r.EndOff
		s.pendAt = time.Now()
	} else {
		b.add(s, e, r.EndOff)
	}
	full := b.bytes >= b.cfg.MaxBytes || b.lines >= b.cfg.MaxLines
	b.mu.Unlock()
	if full { // size-triggered: multiline groups stay pending
		b.flush(false, false)
	}
}

func (b *Batcher) add(s *section, e entry, end int64) {
	s.entries = append(s.entries, e)
	s.end = end
	b.bytes += len(e.msg) + 40
	b.lines++
}

// StreamEnd implements tail.Sink.
func (b *Batcher) StreamEnd(meta cri.Meta, key string) {
	b.mu.Lock()
	s := b.secs[key]
	if s == nil {
		s = &section{meta: meta, file: key}
		b.secs[key] = s
		b.order = append(b.order, key)
	}
	s.ended = true
	b.mu.Unlock()
}

// flush encodes pending sections into one batch. withPending (the interval flush) also
// emits multiline groups that have not grown for MultiTimeout; final emits all of them
// (shutdown). A size-triggered flush keeps every group pending.
func (b *Batcher) flush(withPending, final bool) {
	b.mu.Lock()
	if withPending {
		now := time.Now()
		for _, k := range b.order {
			s := b.secs[k]
			if s.pend != nil && (final || s.ended || now.Sub(s.pendAt) >= b.cfg.MultiTimeout) {
				b.add(s, *s.pend, s.pendEnd)
				s.pend = nil
			}
		}
	}
	if b.lines == 0 && !anyEnded(b.secs) {
		b.mu.Unlock()
		return
	}
	var nd bytes.Buffer
	enc := json.NewEncoder(&nd)
	bt := &Batch{}
	next := map[string]*section{}
	var order []string
	for _, k := range b.order {
		s := b.secs[k]
		if len(s.entries) == 0 && !s.ended {
			if s.pend != nil { // keep multiline pending for the next batch
				next[k] = s
				order = append(order, k)
			}
			continue
		}
		var labels map[string]string
		if b.LabelsFor != nil {
			labels = b.LabelsFor(s.meta.UID)
		}
		enc.Encode(wire.Section{Key: s.meta.StreamKey(), File: s.file, Off: [2]int64{s.start, s.end}, Restart: s.meta.Restart, End: s.ended, Labels: labels})
		for _, e := range s.entries {
			st := "o"
			if e.stderr {
				st = "e"
			}
			enc.Encode(wire.Entry{T: e.t, S: st, M: string(e.msg)})
		}
		bt.Lines += len(s.entries)
		bt.Acks = append(bt.Acks, Ack{Key: s.file, EndOff: s.end})
		if s.pend != nil && !s.ended {
			// carry the multiline group over; the next section starts after what we sent
			ns := &section{meta: s.meta, file: s.file, start: s.end, pend: s.pend, pendN: s.pendN, pendEnd: s.pendEnd, pendAt: s.pendAt, tokens: s.tokens, last: s.last}
			next[k] = ns
			order = append(order, k)
		}
	}
	b.secs = next
	b.order = order
	b.bytes = 0
	b.lines = 0
	bt.Raw = nd.Len()
	bt.Body = b.enc.EncodeAll(nd.Bytes(), nil)
	b.mu.Unlock()
	b.out <- bt // blocks when the shipper is behind → tailers block in Record → backpressure
}

func anyEnded(m map[string]*section) bool {
	for _, s := range m {
		if s.ended {
			return true
		}
	}
	return false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
