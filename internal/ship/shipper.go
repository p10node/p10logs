package ship

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Shipper delivers batches: direct when the hub is healthy, via spool otherwise.
// A batch is acknowledged (checkpoints advanced) once it is either accepted by the
// hub or durably written to the spool.
type Shipper struct {
	Client *Client
	Spool  *Spool
	OnAck  func([]Ack)
	Log    *slog.Logger
	Sent   int64
	Failed int64
}

// Run consumes batches until in is closed. The spool is retried on a timer as
// well, so queued data reaches the hub even when no new lines arrive.
func (s *Shipper) Run(ctx context.Context, in <-chan *Batch) {
	backoff := time.Second
	var notBefore time.Time // next allowed spool drain attempt
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	drain := func() bool {
		if s.Spool.Empty() {
			return true
		}
		if time.Now().Before(notBefore) {
			return false
		}
		if err := s.Spool.Drain(ctx, s.Client.Push0, s.Log); err != nil {
			s.Log.Warn("spool drain failed", "err", err, "spool_bytes", s.Spool.Size(), "retry_in", backoff)
			notBefore = time.Now().Add(backoff)
			backoff = min(backoff*2, 30*time.Second)
			return false
		}
		backoff = time.Second
		s.Log.Info("spool drained")
		return true
	}
	for {
		select {
		case <-tick.C:
			drain()
		case b, ok := <-in:
			if !ok {
				// input closed (shutdown): best-effort drain of the spool
				dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = s.Spool.Drain(dctx, s.Client.Push0, s.Log)
				return
			}
			// keep ordering: while the spool has data, new batches go behind it
			if !drain() {
				s.spool(b)
				continue
			}
			if err := s.send(ctx, b.Body); err != nil {
				if errors.Is(err, ErrTooLarge) {
					s.Log.Error("batch rejected as too large; dropping", "bytes", len(b.Body), "lines", b.Lines)
					s.OnAck(b.Acks) // nothing more we can do with it
					continue
				}
				s.Log.Warn("push failed, spooling", "err", err, "spool_bytes", s.Spool.Size())
				s.spool(b)
				notBefore = time.Now().Add(backoff)
				backoff = min(backoff*2, 30*time.Second)
				continue
			}
			backoff = time.Second
			s.Sent++
			s.OnAck(b.Acks)
		}
	}
}

// send tries up to 3 quick attempts before giving up on direct delivery.
func (s *Shipper) send(ctx context.Context, body []byte) error {
	var err error
	for i := 0; i < 3; i++ {
		if err = s.Client.Push(ctx, body, ""); err == nil {
			return nil
		}
		if errors.Is(err, ErrTooLarge) {
			return err
		}
		s.Failed++
		sleepCtx(ctx, time.Duration(i+1)*500*time.Millisecond)
		if ctx.Err() != nil {
			return err
		}
	}
	return err
}

func (s *Shipper) spool(b *Batch) {
	if err := s.Spool.Put(b.Body); err != nil {
		s.Log.Error("spool write failed; batch lost", "err", err)
		return
	}
	s.OnAck(b.Acks) // durable on disk → safe to advance checkpoints
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Push0 is Push without stats (matches the Drain callback signature).
func (c *Client) Push0(ctx context.Context, body []byte) error { return c.Push(ctx, body, "") }
