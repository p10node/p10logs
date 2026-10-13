package ui

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDevHandlerReload(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("<body>before</body>")
	h := DevHandler(dir)
	get := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		return r
	}
	first := get("/__dev/revision").Body.String()
	page := get("/")
	if page.Code != 200 || page.Header().Get("Cache-Control") != "no-store" || !strings.Contains(page.Body.String(), first) {
		t.Fatalf("page must embed its uncached revision: %s", page.Body.String())
	}
	write("<body>after</body>")
	if next := get("/__dev/revision").Body.String(); next == first {
		t.Fatal("UI edit did not change revision")
	}
	if page := get("/index.html").Body.String(); !strings.Contains(page, "after") || !strings.Contains(page, "location.reload()") {
		t.Fatalf("UI edit was not served with reload script: %s", page)
	}
	other := httptest.NewRecorder()
	DevHandler(dir).ServeHTTP(other, httptest.NewRequest(http.MethodGet, "/__dev/revision", nil))
	if other.Body.String() == get("/__dev/revision").Body.String() {
		t.Fatal("new server must have a new revision")
	}
	if r := get("/../go.mod"); r.Code != http.StatusNotFound {
		t.Fatal("dev handler must not serve arbitrary files")
	}
	if err := os.Remove(filepath.Join(dir, "index.html")); err != nil {
		t.Fatal(err)
	}
	if r := get("/"); r.Code != http.StatusInternalServerError {
		t.Fatal("missing UI must return a useful error")
	}
}
