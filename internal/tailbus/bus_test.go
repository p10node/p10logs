package tailbus

import (
	"sync"
	"testing"
	"time"
)

func drain(ch chan Event) []Event {
	var out []Event
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestSubscribeCountUnsubscribe(t *testing.T) {
	b := New()
	if b.Count() != 0 {
		t.Fatalf("new bus count %d", b.Count())
	}
	b.Publish(1, []Event{{StreamID: 1, Msg: "nobody listening"}}) // must not panic or block
	s1 := b.Subscribe(4, func(int64) bool { return true })
	s2 := b.Subscribe(4, func(int64) bool { return true })
	if b.Count() != 2 || cap(s1.Ch) != 4 {
		t.Fatalf("count %d cap %d", b.Count(), cap(s1.Ch))
	}
	b.Unsubscribe(s1)
	b.Unsubscribe(s1) // idempotent
	if b.Count() != 1 {
		t.Fatalf("count after unsubscribe %d", b.Count())
	}
	b.Publish(7, []Event{{StreamID: 7, Msg: "x"}})
	if got := drain(s1.Ch); len(got) != 0 {
		t.Fatalf("unsubscribed still received %v", got)
	}
	if got := drain(s2.Ch); len(got) != 1 || got[0].Msg != "x" {
		t.Fatalf("remaining subscriber got %v", got)
	}
}

func TestPublishFanOutAndMatch(t *testing.T) {
	b := New()
	all := b.Subscribe(16, func(int64) bool { return true })
	only2 := b.Subscribe(16, func(id int64) bool { return id == 2 })
	none := b.Subscribe(16, func(int64) bool { return false })
	evs1 := []Event{{StreamID: 1, TS: 10, Msg: "a"}, {StreamID: 1, TS: 11, Msg: "b", Stderr: true}}
	evs2 := []Event{{StreamID: 2, TS: 12, Msg: "c", Restart: 3}}
	b.Publish(1, evs1)
	b.Publish(2, evs2)

	got := drain(all.Ch)
	if len(got) != 3 || got[0].Msg != "a" || got[1].Msg != "b" || !got[1].Stderr || got[2].Msg != "c" || got[2].Restart != 3 {
		t.Fatalf("all: %+v", got)
	}
	if got := drain(only2.Ch); len(got) != 1 || got[0].StreamID != 2 {
		t.Fatalf("only2: %+v", got)
	}
	if got := drain(none.Ch); len(got) != 0 {
		t.Fatalf("none: %+v", got)
	}
	for _, s := range []*Sub{all, only2, none} {
		if s.Dropped.Load() != 0 {
			t.Fatalf("unexpected drops %d", s.Dropped.Load())
		}
	}
}

func TestSlowSubscriberDropsInsteadOfBlocking(t *testing.T) {
	b := New()
	slow := b.Subscribe(2, func(int64) bool { return true })
	fast := b.Subscribe(10, func(int64) bool { return true })
	evs := make([]Event, 5)
	for i := range evs {
		evs[i] = Event{StreamID: 1, TS: int64(i)}
	}
	done := make(chan struct{})
	go func() { b.Publish(1, evs); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a full subscriber")
	}
	got := drain(slow.Ch)
	if len(got) != 2 || got[0].TS != 0 || got[1].TS != 1 {
		t.Fatalf("slow kept %+v", got)
	}
	if slow.Dropped.Load() != 3 {
		t.Fatalf("slow dropped %d want 3", slow.Dropped.Load())
	}
	if len(drain(fast.Ch)) != 5 || fast.Dropped.Load() != 0 {
		t.Fatal("fast subscriber affected by the slow one")
	}
	// once drained, the slow subscriber receives again and the drop count keeps accumulating
	b.Publish(1, evs[:3])
	if len(drain(slow.Ch)) != 2 || slow.Dropped.Load() != 4 {
		t.Fatalf("second round dropped %d", slow.Dropped.Load())
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	b := New()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					b.Publish(int64(i), []Event{{StreamID: int64(i)}})
				}
			}
		}()
	}
	var received int
	for i := 0; i < 50; i++ {
		s := b.Subscribe(8, func(int64) bool { return true })
		time.Sleep(time.Millisecond)
		received += len(drain(s.Ch))
		b.Unsubscribe(s)
	}
	close(stop)
	wg.Wait()
	if b.Count() != 0 {
		t.Fatalf("leaked subscribers %d", b.Count())
	}
	if received == 0 {
		t.Fatal("no events observed under concurrency")
	}
}
