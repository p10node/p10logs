package tail

import (
	"bytes"
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
	for i := 0; i < 200 && sink.ends == 0; i++ { // gone on two checks: up to ~3 s at the default poll
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

// The tailer's own 2 s inode check must not stop reading the old inode when the path
// already points at the new file: the runtime may still write to the old one until it
// reopens. Regression: lines appended after the replacement were lost ~1 in 8 rotations.
func TestTailerDrainsOldInodeAfterReplacement(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "0.log")
	os.WriteFile(p, []byte(line(0)), 0o644)
	st, _ := os.Stat(p)
	sink := &memSink{}
	tl := NewTailer(p, Inode(st), cri.Meta{Namespace: "ns", Pod: "pod", UID: "uid", Container: "app"}, 0, sink)
	tl.Poll = 5 * time.Millisecond // inode check every ~40 ms
	tl.RotateWait = 150 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tl.Run(ctx)
	for i := 0; i < 100 && sink.count() < 1; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	// rotate without telling the tailer (the discoverer may be slower than its own check)
	os.Rename(p, p+".20260930-100000")
	os.WriteFile(p, []byte(line(1)), 0o644)
	time.Sleep(80 * time.Millisecond) // the tailer has now seen the path point elsewhere
	f, _ := os.OpenFile(p+".20260930-100000", os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(line(2))
	f.Close()
	select {
	case <-tl.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tailer did not finish after the rotation grace period")
	}
	if sink.count() != 2 || sink.msgs[1] != "line c" {
		t.Fatalf("want [line a, line c] from the old inode, got %v", sink.msgs)
	}
	if sink.ends != 0 {
		t.Fatal("rotation must not end the stream")
	}
}

// A path that is briefly absent (between rename and reopen) is not a deleted pod.
func TestTailerEndsOnlyWhenPathStaysGone(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "0.log")
	os.WriteFile(p, []byte(line(0)), 0o644)
	st, _ := os.Stat(p)
	sink := &memSink{}
	tl := NewTailer(p, Inode(st), cri.Meta{Namespace: "ns", Pod: "pod", UID: "uid", Container: "app"}, 0, sink)
	tl.Poll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tl.Run(ctx)
	for i := 0; i < 100 && sink.count() < 1; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	os.Rename(p, p+".tmp") // path absent for at most one check (re-check comes 20 ms later)
	time.Sleep(12 * time.Millisecond)
	os.Rename(p+".tmp", p)
	time.Sleep(100 * time.Millisecond)
	if sink.ends != 0 {
		t.Fatal("transient absence ended the stream")
	}
	os.Remove(p)
	select {
	case <-tl.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("tailer did not finish after deletion")
	}
	if sink.ends != 1 {
		t.Fatalf("ends=%d want 1", sink.ends)
	}
}

// The kernel reuses a freed inode: the next 0.log can get the inode of a rotated file the
// agent already read to the end. The checkpoint must not carry over (it would skip the
// whole new file and, worse, the hub's cursor would drop every line as a duplicate).
func TestTailerReusedInodeIsANewFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "0.log")
	os.WriteFile(p, []byte(line(0)+line(1)), 0o644)
	st, _ := os.Stat(p)
	pos, _ := LoadPositions(filepath.Join(dir, "pos.json"))
	sink := &memSink{}
	run := func() {
		tl := NewTailer(p, Inode(st), cri.Meta{Namespace: "ns", Pod: "pod", UID: "uid", Container: "app"}, 0, sink)
		tl.Positions = pos
		tl.Poll = 5 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		go tl.Run(ctx)
		for i := 0; i < 100 && tl.Key() == ""; i++ {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(60 * time.Millisecond)
		cancel()
		select {
		case <-tl.Done():
		case <-time.After(time.Second):
			t.Fatal("tailer did not stop")
		}
		// what the agent would checkpoint after acks
		pos.Set(tl.Key(), Pos{Offset: tl.Offset()})
	}
	run()
	if sink.count() != 2 {
		t.Fatalf("first file: %v", sink.msgs)
	}
	// same path, same inode, different content (truncate + rewrite stands in for inode reuse)
	os.WriteFile(p, []byte("2026-09-30T11:00:00.000000000Z stdout F fresh\n"), 0o644)
	run()
	if sink.count() != 3 || sink.msgs[2] != "fresh" {
		t.Fatalf("reused inode must be read from the start: %v", sink.msgs)
	}
	// and the old content's checkpoint still resumes the old content
	os.WriteFile(p, []byte(line(0)+line(1)+line(2)), 0o644)
	run()
	if sink.count() != 4 || sink.msgs[3] != "line c" {
		t.Fatalf("checkpoint for the original first line must resume after line b: %v", sink.msgs)
	}
}

// A checkpoint written by an agent before FileKey existed (path@inode) is adopted once.
func TestTailerAdoptsLegacyCheckpoint(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "0.log")
	os.WriteFile(p, []byte(line(0)+line(1)+line(2)), 0o644)
	st, _ := os.Stat(p)
	pos, _ := LoadPositions(filepath.Join(dir, "pos.json"))
	pos.Set(Key(p, Inode(st)), Pos{Offset: int64(len(line(0)) + len(line(1)))})
	sink := &memSink{}
	tl := NewTailer(p, Inode(st), cri.Meta{Namespace: "ns", Pod: "pod", UID: "uid", Container: "app"}, 0, sink)
	tl.Positions = pos
	tl.Poll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tl.Run(ctx)
	time.Sleep(150 * time.Millisecond)
	if sink.count() != 1 || sink.msgs[0] != "line c" {
		t.Fatalf("legacy resume: %v", sink.msgs)
	}
	if tl.Key() == Key(p, Inode(st)) || tl.Key() == "" {
		t.Fatalf("new key expected, got %q", tl.Key())
	}
}

// In-place truncation (CRI-O log_size_max) ends the tailer so the discoverer re-keys the file.
func TestTailerTruncationStartsNewIdentity(t *testing.T) {
	dir := t.TempDir()
	podDir := filepath.Join(dir, "ns_pod_uid", "app")
	os.MkdirAll(podDir, 0o755)
	p := filepath.Join(podDir, "0.log")
	os.WriteFile(p, []byte(line(0)+line(1)), 0o644)
	pos, _ := LoadPositions(filepath.Join(dir, "pos.json"))
	sink := &memSink{}
	d := &Discoverer{Dir: dir, Positions: pos, Sink: sink, Interval: 30 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)
	for i := 0; i < 100 && sink.count() < 2; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	os.WriteFile(p, []byte("2026-09-30T12:00:00.000000000Z stdout F after-truncate\n"), 0o644) // O_TRUNC, same inode
	for i := 0; i < 400 && sink.count() < 3; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if sink.count() != 3 || sink.msgs[2] != "after-truncate" {
		t.Fatalf("after truncation: %v", sink.msgs)
	}
}

// A first line longer than 4 KiB (containerd writes 16 KiB partial records) must not stall
// identity detection. Regression: the tailer waited forever for a newline in the first 4 KiB.
func TestTailerLongFirstLine(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "0.log")
	big := "2026-09-30T10:00:00.000000000Z stdout P " + string(bytes.Repeat([]byte("x"), 16384)) + "\n" +
		"2026-09-30T10:00:00.000000001Z stdout F tail-of-big\n" + line(1)
	os.WriteFile(p, []byte(big), 0o644)
	st, _ := os.Stat(p)
	sink := &memSink{}
	tl := NewTailer(p, Inode(st), cri.Meta{Namespace: "ns", Pod: "pod", UID: "uid", Container: "app"}, 0, sink)
	tl.Poll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tl.Run(ctx)
	for i := 0; i < 200 && sink.count() < 2; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if sink.count() != 2 || len(sink.msgs[0]) != 16384+len("tail-of-big") || sink.msgs[1] != "line b" {
		t.Fatalf("long first line: count=%d lens=%v", sink.count(), func() []int {
			var o []int
			for _, m := range sink.msgs {
				o = append(o, len(m))
			}
			return o
		}())
	}
	if tl.Key() == "" {
		t.Fatal("key not set")
	}
}
