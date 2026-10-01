package tail

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/p10node/p10logs/internal/cri"
)

type memSink struct {
	mu   sync.Mutex
	msgs []string
	ends int
}

func (m *memSink) Record(r Record) {
	m.mu.Lock()
	m.msgs = append(m.msgs, string(r.Msg))
	m.mu.Unlock()
}
func (m *memSink) StreamEnd(cri.Meta, string) { m.mu.Lock(); m.ends++; m.mu.Unlock() }
func (m *memSink) count() int                 { m.mu.Lock(); defer m.mu.Unlock(); return len(m.msgs) }

func line(i int) string {
	return "2026-09-30T10:00:00.000000000Z stdout F line " + string(rune('a'+i%26)) + "\n"
}

func TestTailRotateAndDelete(t *testing.T) {
	dir := t.TempDir()
	podDir := filepath.Join(dir, "ns_pod_uid", "app")
	os.MkdirAll(podDir, 0o755)
	p := filepath.Join(podDir, "0.log")
	os.WriteFile(p, []byte(line(0)+line(1)), 0o644)
	pos, _ := LoadPositions(filepath.Join(dir, "pos.json"))
	sink := &memSink{}
	d := &Discoverer{Dir: dir, Positions: pos, Sink: sink, Interval: 50 * time.Millisecond, RotateWait: 300 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	wait := func(n int) {
		for i := 0; i < 100 && sink.count() < n; i++ {
			time.Sleep(20 * time.Millisecond)
		}
		if sink.count() != n {
			t.Fatalf("want %d lines, got %d: %v", n, sink.count(), sink.msgs)
		}
	}
	wait(2)
	// append
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(line(2))
	f.Close()
	wait(3)
	// kubelet-style rotation: rename, then recreate same path
	os.Rename(p, p+".20260930-100000")
	os.WriteFile(p, []byte(line(3)), 0o644)
	// write to the rotated file too (runtime may still flush before reopen)
	f, _ = os.OpenFile(p+".20260930-100000", os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(line(4))
	f.Close()
	wait(5)
	// delete the pod directory
	os.RemoveAll(filepath.Join(dir, "ns_pod_uid"))
	for i := 0; i < 100 && sink.ends == 0; i++ {
		time.Sleep(30 * time.Millisecond)
	}
	if sink.ends == 0 {
		t.Fatal("expected StreamEnd after delete")
	}
	cancel()
}

func TestTailResumeFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	podDir := filepath.Join(dir, "ns_pod_uid", "app")
	os.MkdirAll(podDir, 0o755)
	p := filepath.Join(podDir, "0.log")
	os.WriteFile(p, []byte(line(0)+line(1)+line(2)), 0o644)
	st, _ := os.Stat(p)
	pos, _ := LoadPositions(filepath.Join(dir, "pos.json"))
	pos.Set(Key(p, Inode(st)), Pos{Offset: int64(len(line(0)) + len(line(1)))})
	sink := &memSink{}
	d := &Discoverer{Dir: dir, Positions: pos, Sink: sink, Interval: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)
	time.Sleep(300 * time.Millisecond)
	cancel()
	if sink.count() != 1 || sink.msgs[0] != "line c" {
		t.Fatalf("resume: %v", sink.msgs)
	}
}
