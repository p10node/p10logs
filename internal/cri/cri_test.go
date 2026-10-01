package cri

import (
	"strings"
	"testing"
)

func TestParsePath(t *testing.T) {
	cases := map[string]Meta{
		"/var/log/pods/kube-system_coredns-76f_1a2b-3c/coredns/0.log":                      {Namespace: "kube-system", Pod: "coredns-76f", UID: "1a2b-3c", Container: "coredns"},
		"/var/log/pods/payments_api-1_uid/app/3.log.20260930-101500":                       {Namespace: "payments", Pod: "api-1", UID: "uid", Container: "app", Restart: 3, Rotated: "20260930-101500"},
		"/var/log/pods/payments_api-1_uid/app/3.log.20260930-101500.gz":                    {Namespace: "payments", Pod: "api-1", UID: "uid", Container: "app", Restart: 3, Rotated: "20260930-101500", Gzip: true},
		"/var/log/pods/kube-system_etcd-node1_9f8e7d6c5b4a3f2e1d0c9b8a7f6e5d4c/etcd/0.log": {Namespace: "kube-system", Pod: "etcd-node1", UID: "9f8e7d6c5b4a3f2e1d0c9b8a7f6e5d4c", Container: "etcd"},
	}
	for p, want := range cases {
		got, ok := ParsePath(p)
		if !ok || got != want {
			t.Errorf("%s: got %+v ok=%v want %+v", p, got, ok, want)
		}
	}
	if _, ok := ParsePath("/var/log/pods/x/y.log"); ok {
		t.Error("expected no match")
	}
}

func TestParseCRI(t *testing.T) {
	l, err := Parse([]byte("2016-10-06T00:17:09.669794202Z stdout F The content of the log entry 1\n"))
	if err != nil || l.Stderr || l.Partial || string(l.Msg) != "The content of the log entry 1" {
		t.Fatalf("got %+v %v", l, err)
	}
	l, err = Parse([]byte("2016-10-06T00:17:09.669794202Z stderr P:tag partial"))
	if err != nil || !l.Stderr || !l.Partial || string(l.Msg) != "partial" {
		t.Fatalf("got %+v %v", l, err)
	}
	if _, err := Parse([]byte("garbage")); err == nil {
		t.Fatal("expected error")
	}
	// empty message with tag only
	l, err = Parse([]byte("2016-10-06T00:17:09.669794202Z stdout F"))
	if err != nil || len(l.Msg) != 0 {
		t.Fatalf("got %+v %v", l, err)
	}
}

func TestParseDocker(t *testing.T) {
	l, err := Parse([]byte(`{"log":"hello \"w\"\n","stream":"stderr","time":"2026-09-30T10:00:00.123456789Z"}`))
	if err != nil || !l.Stderr || l.Partial || string(l.Msg) != `hello "w"` {
		t.Fatalf("got %+v %v", l, err)
	}
}

func TestMerger(t *testing.T) {
	var m Merger
	big := strings.Repeat("x", 16384)
	if _, ok := m.Push(Line{Partial: true, Msg: []byte(big)}); ok {
		t.Fatal("should buffer")
	}
	if _, ok := m.Push(Line{Partial: true, Msg: []byte(big)}); ok {
		t.Fatal("should buffer")
	}
	out, ok := m.Push(Line{Partial: false, Msg: []byte("tail")})
	if !ok || len(out.Msg) != 2*16384+4 {
		t.Fatalf("merged len %d ok=%v", len(out.Msg), ok)
	}
	out, ok = m.Push(Line{Msg: []byte("plain")})
	if !ok || string(out.Msg) != "plain" {
		t.Fatal("plain line should pass through")
	}
	m.MaxSize = 10
	m.Push(Line{Partial: true, Msg: []byte("0123456789")})
	if _, ok := m.Push(Line{Partial: true, Msg: []byte("x")}); !ok {
		t.Fatal("should force flush above MaxSize")
	}
}
