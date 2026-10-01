package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/p10node/p10logs/internal/chunk"
)

func TestAppendReplayTruncate(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, func(Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := l.Append(chunk.Meta{Key: "a/b/c/d", File: "f", Off: [2]int64{int64(i * 10), int64(i*10 + 10)}, Count: 1}, []chunk.Entry{{TS: int64(i), Msg: []byte("line")}}); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	// corrupt tail
	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	f, _ := os.OpenFile(segs[0], os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte("junk"))
	f.Close()
	var got []uint64
	l2, err := Open(dir, func(r Record) error { got = append(got, r.Seq); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2] != 3 {
		t.Fatalf("replay %v", got)
	}
	seq, _ := l2.Append(chunk.Meta{Key: "k"}, nil)
	if seq != 4 {
		t.Fatalf("seq after replay = %d", seq)
	}
	if err := l2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	segs, _ = filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segs) != 1 || l2.Size() != 0 {
		t.Fatalf("checkpoint left %v size %d", segs, l2.Size())
	}
	l2.Close()
}
