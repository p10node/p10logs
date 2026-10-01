package query

import "testing"

func TestFilter(t *testing.T) {
	line := []byte(`{"level":"error","msg":"GET /v1/charges 502 upstream timeout after 900ms","user":42}`)
	cases := map[string]bool{
		``:                      true,
		`error`:                 true,
		`ERROR timeout`:         true,
		`!healthz`:              true,
		`!timeout`:              false,
		`"upstream timeout"`:    true,
		`"timeout upstream"`:    false,
		`/timeout after \d+ms/`: true,
		`/timeout after \d+s/`:  false,
		`level=error`:           true,
		`level=warn`:            false,
		`user=42`:               true,
		`!level=error`:          false,
		`error !healthz level=error user=42 /charges/`: true,
		`/(unclosed`: true, // invalid regex ignored
	}
	for q, want := range cases {
		if got := Compile(q).Match(line); got != want {
			t.Errorf("%q: got %v want %v", q, got, want)
		}
	}
	if Compile("plain").Match([]byte("no json here, plain text")) != true {
		t.Error("plain")
	}
	if Compile("level=error").Match([]byte("not json level=error")) {
		t.Error("kv on non-json must not match")
	}
	w := Compile(`error "two words" !no /re/ k=v`).Words()
	if len(w) != 2 || w[0] != "error" || w[1] != "two words" {
		t.Errorf("words %v", w)
	}
}
