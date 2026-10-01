package s3

import (
	"encoding/xml"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Fake is an in-memory S3-compatible server for tests (path-style, no auth checks
// beyond requiring an Authorization header).
type Fake struct {
	mu   sync.Mutex
	objs map[string][]byte
}

// NewFake creates an empty fake.
func NewFake() *Fake { return &Fake{objs: map[string][]byte{}} }

// Len returns the number of stored objects.
func (f *Fake) Len() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.objs) }

func (f *Fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		http.Error(w, "no sigv4", 403)
		return
	}
	// path-style: /bucket/key
	p := strings.TrimPrefix(r.URL.Path, "/")
	i := strings.IndexByte(p, '/')
	key := ""
	if i >= 0 {
		key = p[i+1:]
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objs {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		type c struct {
			Key  string `xml:"Key"`
			Size int64  `xml:"Size"`
		}
		var out struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			IsTruncated bool     `xml:"IsTruncated"`
			Contents    []c      `xml:"Contents"`
		}
		for _, k := range keys {
			out.Contents = append(out.Contents, c{k, int64(len(f.objs[k]))})
		}
		xml.NewEncoder(w).Encode(out)
	case r.Method == http.MethodPut && key == "":
		w.WriteHeader(200) // create bucket
	case r.Method == http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.objs[key] = b
		w.WriteHeader(200)
	case r.Method == http.MethodGet:
		b, ok := f.objs[key]
		if !ok {
			http.Error(w, "NoSuchKey", 404)
			return
		}
		if rg := r.Header.Get("Range"); strings.HasPrefix(rg, "bytes=") {
			parts := strings.SplitN(strings.TrimPrefix(rg, "bytes="), "-", 2)
			from, _ := strconv.ParseInt(parts[0], 10, 64)
			to := int64(len(b)) - 1
			if parts[1] != "" {
				to, _ = strconv.ParseInt(parts[1], 10, 64)
			}
			if from > to || to >= int64(len(b)) {
				to = int64(len(b)) - 1
			}
			w.WriteHeader(206)
			w.Write(b[from : to+1])
			return
		}
		w.Write(b)
	case r.Method == http.MethodDelete:
		delete(f.objs, key)
		w.WriteHeader(204)
	default:
		http.Error(w, "unsupported", 400)
	}
}
