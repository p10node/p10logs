package ui

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// DevHandler reads the console on every request. Its revision changes when the
// HTML changes or the hub restarts. Only the hub's explicit --dev-ui flag uses it.
func DevHandler(dir string) http.Handler {
	boot := time.Now().UnixNano()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/index.html" && r.URL.Path != "/__dev/revision" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		page, err := os.ReadFile(filepath.Join(dir, "index.html"))
		if err != nil {
			http.Error(w, "Cannot read development UI index.html", http.StatusInternalServerError)
			return
		}
		revision := fmt.Sprintf("%d-%x", boot, sha256.Sum256(page))
		if r.URL.Path == "/__dev/revision" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprint(w, revision)
			return
		}
		script := fmt.Sprintf(`<script>
(() => {
  const revision = %q;
  async function check() {
    try {
      const response = await fetch('/__dev/revision', {cache: 'no-store'});
      if (response.ok && (await response.text()) !== revision) {
        location.reload();
        return;
      }
    } catch (_) {} // Retry while the hub is rebuilding/restarting.
    setTimeout(check, 1000);
  }
  setTimeout(check, 1000);
})();
</script>`, revision)
		if bytes.Contains(page, []byte("</body>")) {
			page = bytes.Replace(page, []byte("</body>"), []byte(script+"</body>"), 1)
		} else {
			page = append(page, []byte(script)...)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})
}
