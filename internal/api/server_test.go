package api

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	now := time.Now().UnixNano()
	def := int64(-1)
	within := func(got, want int64, tol time.Duration) bool {
		d := got - want
		return d < int64(tol) && d > -int64(tol)
	}
	cases := []struct {
		in   string
		want int64 // expected, relative values computed from now
	}{
		{"-15m", now - int64(15*time.Minute)},
		{"-1h", now - int64(time.Hour)},
		{"-7d", now - int64(7*24*time.Hour)},
		{"-1.5d", now - int64(36*time.Hour)},
		{"-2w", now - int64(14*24*time.Hour)},
		{"now", now},
		{"1700000000", 1700000000 * 1e9},
		{"1700000000123", 1700000000123 * 1e6},
		{"1700000000123456789", 1700000000123456789},
		{"2026-10-04T10:00:00Z", time.Date(2026, 10, 4, 10, 0, 0, 0, time.UTC).UnixNano()},
	}
	for _, c := range cases {
		if got := parseTime(c.in, def); !within(got, c.want, 2*time.Second) {
			t.Errorf("%s: got %d want ≈ %d", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "-7x", "-d", "yesterday", "--1h"} {
		if got := parseTime(bad, def); got != def {
			t.Errorf("%q: got %d, want default", bad, got)
		}
	}
}
