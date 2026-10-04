package ship

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/p10node/p10logs/internal/cri"
	"github.com/p10node/p10logs/internal/tail"
	"github.com/p10node/p10logs/internal/wire"
)

var testMeta = cri.Meta{Namespace: "ns", Pod: "p", UID: "u", Container: "c"}

func rec(b *Batcher, off int64, msg string) {
	b.Record(tail.Record{Meta: testMeta, Key: "f", Time: time.Unix(0, off), Msg: []byte(msg), StartOff: off, EndOff: off + 10})
}

func decode(t *testing.T, bt *Batch) (secs []wire.Section, ents []wire.Entry) {
	t.Helper()
	dec, _ := zstd.NewReader(nil)
	raw, err := dec.DecodeAll(bt.Body, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		var probe struct {
			K *string `json:"k"`
		}
		if json.Unmarshal(sc.Bytes(), &probe) == nil && probe.K != nil {
			var s wire.Section
			json.Unmarshal(sc.Bytes(), &s)
			secs = append(secs, s)
			continue
		}
		var e wire.Entry
		json.Unmarshal(sc.Bytes(), &e)
		ents = append(ents, e)
	}
	return
}

func take(t *testing.T, b *Batcher) *Batch {
	t.Helper()
	select {
	case bt := <-b.Out():
		return bt
	case <-time.After(time.Second):
		t.Fatal("no batch emitted")
		return nil
	}
}

func none(t *testing.T, b *Batcher) {
	t.Helper()
	select {
	case bt := <-b.Out():
		t.Fatalf("unexpected batch with %d lines", bt.Lines)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestMultilineJoinTimeoutAndEnd(t *testing.T) {
	b := NewBatcher(BatcherConfig{Multiline: true, StartPattern: `^\d{4}-`, MultiMaxLines: 10, MultiTimeout: 80 * time.Millisecond}, 4)
	rec(b, 0, "2026-01-01 boom")
	rec(b, 10, "\tat a")
	rec(b, 20, "\tat b")
	b.flush(true, false) // interval flush while the group is still fresh: keep it pending
	none(t, b)

	time.Sleep(100 * time.Millisecond)
	b.flush(true, false) // quiet for longer than MultiTimeout: emit
	secs, ents := decode(t, take(t, b))
	if len(ents) != 1 || ents[0].M != "2026-01-01 boom\n\tat a\n\tat b" {
		t.Fatalf("joined group: %+v", ents)
	}
	if len(secs) != 1 || secs[0].Off != [2]int64{0, 30} || secs[0].Key != "ns/p/u/c" {
		t.Fatalf("section: %+v", secs)
	}

	// a new start line closes the previous group immediately
	rec(b, 30, "2026-01-01 second")
	rec(b, 40, "\tat c")
	rec(b, 50, "2026-01-01 third")
	b.flush(true, false)
	secs, ents = decode(t, take(t, b))
	if len(ents) != 1 || ents[0].M != "2026-01-01 second\n\tat c" {
		t.Fatalf("second group: %+v", ents)
	}
	if secs[0].Off != [2]int64{30, 50} {
		t.Fatalf("section offsets: %+v", secs[0].Off)
	}

	// the file disappearing (pod deleted) flushes the pending group before the timeout
	b.StreamEnd(testMeta, "f")
	b.flush(true, false)
	secs, ents = decode(t, take(t, b))
	if len(ents) != 1 || ents[0].M != "2026-01-01 third" || !secs[0].End || secs[0].Off != [2]int64{50, 60} {
		t.Fatalf("ended stream: secs=%+v ents=%+v", secs, ents)
	}
}

func TestMultilineSizeFlushKeepsGroup(t *testing.T) {
	b := NewBatcher(BatcherConfig{Multiline: true, StartPattern: `^\d{4}-`, MultiMaxLines: 10, MaxLines: 2}, 4)
	rec(b, 0, "2026-01-01 one")
	rec(b, 10, "2026-01-01 two")   // closes "one"
	rec(b, 20, "2026-01-01 three") // closes "two" → 2 lines → size flush; "three" stays pending
	_, ents := decode(t, take(t, b))
	if len(ents) != 2 || ents[0].M != "2026-01-01 one" || ents[1].M != "2026-01-01 two" {
		t.Fatalf("size flush: %+v", ents)
	}
	rec(b, 30, "\tcontinues three")
	b.flush(true, true)
	_, ents = decode(t, take(t, b))
	if len(ents) != 1 || ents[0].M != "2026-01-01 three\n\tcontinues three" {
		t.Fatalf("carried group: %+v", ents)
	}
}

func TestRateLimitMarker(t *testing.T) {
	b := NewBatcher(BatcherConfig{LinesPerSec: 2}, 4)
	for i := 0; i < 10; i++ {
		rec(b, int64(i*10), "line")
	}
	time.Sleep(600 * time.Millisecond) // refill ≥ 1 token
	rec(b, 100, "after")
	b.flush(true, false)
	secs, ents := decode(t, take(t, b))
	if b.Dropped != 8 {
		t.Fatalf("dropped=%d want 8", b.Dropped)
	}
	if len(ents) != 4 || ents[2].M != "p10logs: dropped 8 lines (rate limit)" || ents[2].S != "e" || ents[3].M != "after" {
		t.Fatalf("entries: %+v", ents)
	}
	if secs[0].Off != [2]int64{0, 110} { // dropped lines still advance the checkpoint
		t.Fatalf("offsets: %+v", secs[0].Off)
	}
}
