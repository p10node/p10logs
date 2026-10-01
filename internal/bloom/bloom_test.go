package bloom

import "testing"

func TestBloom(t *testing.T) {
	f := New()
	f.Add([]byte(`{"level":"error","msg":"GET /v1/charges 502 upstream timeout after 900ms","user_id":42}`))
	yes := []string{"error", "ERROR", "upstream timeout", "charges", "user_id", "rror", "900ms", "ms", "x"}
	no := []string{"warning", "healthz", "database", "upstream timeouts"}
	for _, s := range yes {
		if !f.MayContain(s) {
			t.Errorf("false negative for %q", s)
		}
	}
	for _, s := range no {
		if f.MayContain(s) {
			t.Errorf("unexpected positive for %q", s)
		}
	}
	g, err := Unmarshal(f.Marshal())
	if err != nil || !g.MayContain("timeout") || g.MayContain("healthz") {
		t.Fatal("roundtrip", err)
	}
}
