// Package tailbus fans freshly ingested lines out to live-tail subscribers.
package tailbus

import (
	"sync"
	"sync/atomic"
)

// Event is one line for a subscriber.
type Event struct {
	StreamID int64
	TS       int64
	Stderr   bool
	Msg      string
	Restart  int
}

// Sub is one subscriber; Match is consulted per stream id.
type Sub struct {
	Ch      chan Event
	Match   func(streamID int64) bool
	Dropped atomic.Int64
}

// Bus is the in-process publisher.
type Bus struct {
	mu   sync.RWMutex
	subs map[*Sub]struct{}
}

// New creates a bus.
func New() *Bus { return &Bus{subs: map[*Sub]struct{}{}} }

// Subscribe registers a subscriber with a bounded buffer.
func (b *Bus) Subscribe(buffer int, match func(int64) bool) *Sub {
	s := &Sub{Ch: make(chan Event, buffer), Match: match}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

// Unsubscribe removes a subscriber.
func (b *Bus) Unsubscribe(s *Sub) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
}

// Count returns the number of subscribers.
func (b *Bus) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// Publish delivers events without ever blocking ingest; slow subscribers lose lines
// and are told how many.
func (b *Bus) Publish(streamID int64, evs []Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		if !s.Match(streamID) {
			continue
		}
		for _, e := range evs {
			select {
			case s.Ch <- e:
			default:
				s.Dropped.Add(1)
			}
		}
	}
}
