package chunk

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRoundTripAndRecovery(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "1.open")
	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	ents := []Entry{{TS: 10, Msg: []byte("hello")}, {TS: 11, Stderr: true, Msg: []byte("oops")}}
	if _, err := w.Append(Meta{File: "f", Off: [2]int64{0, 20}, MinTS: 10, MaxTS: 11, Count: 2}, ents); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(Meta{File: "f", Off: [2]int64{20, 30}, MinTS: 12, MaxTS: 12, Count: 1}, []Entry{{TS: 12, Msg: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	// corrupt: append garbage
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte("garbage garbage garbage"))
	f.Close()
	w2, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(w2.Frames) != 2 || w2.Lines != 3 || w2.MinTS != 10 || w2.MaxTS != 12 {
		t.Fatalf("recovered %+v lines=%d", w2.Frames, w2.Lines)
	}
	st, _ := os.Stat(p)
	if st.Size() != w2.Size() {
		t.Fatalf("garbage not truncated: %d vs %d", st.Size(), w2.Size())
	}
	sealed := filepath.Join(dir, "1.chunk")
	ft, err := w2.Seal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	ft2, err := ReadFooter(sealed)
	if err != nil || ft2.Lines != ft.Lines || len(ft2.Frames) != 2 {
		t.Fatalf("footer %+v %v", ft2, err)
	}
	rf, _ := os.Open(sealed)
	defer rf.Close()
	var got []string
	m, err := ReadFrame(rf, ft2.Frames[0], func(e Entry) bool { got = append(got, string(e.Msg)); return true })
	if err != nil || m.Off[1] != 20 || len(got) != 2 || got[1] != "oops" {
		t.Fatalf("read %v %+v %v", got, m, err)
	}
}
