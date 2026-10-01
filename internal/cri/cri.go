// Package cri parses Kubernetes CRI log lines and /var/log/pods paths.
package cri

import (
	"bytes"
	"errors"
	"regexp"
	"strconv"
	"time"
)

// Meta is what the file path alone tells us about a container log file.
type Meta struct {
	Namespace string
	Pod       string
	UID       string
	Container string
	Restart   int
	Rotated   string // "" for the live file, else the rotation timestamp suffix
	Gzip      bool
}

// StreamKey is the per-cluster identity of a stream: ns/pod/uid/container.
func (m Meta) StreamKey() string {
	return m.Namespace + "/" + m.Pod + "/" + m.UID + "/" + m.Container
}

var pathRe = regexp.MustCompile(`^(?:.*/)?([^_/]+)_([^_/]+)_([^/]+)/([^/]+)/(\d+)\.log(?:\.(\d{8}-\d{6}))?(\.gz)?$`)

// ParsePath parses <podsDir>/<ns>_<pod>_<uid>/<container>/<restart>.log[.<ts>][.gz].
func ParsePath(p string) (Meta, bool) {
	m := pathRe.FindStringSubmatch(p)
	if m == nil {
		return Meta{}, false
	}
	r, _ := strconv.Atoi(m[5])
	return Meta{Namespace: m[1], Pod: m[2], UID: m[3], Container: m[4], Restart: r, Rotated: m[6], Gzip: m[7] != ""}, true
}

// Line is one parsed CRI record.
type Line struct {
	Time    time.Time
	Stderr  bool
	Partial bool
	Msg     []byte // not a copy; valid until the next read
}

var ErrFormat = errors.New("cri: malformed line")

// Parse parses `<RFC3339Nano> <stdout|stderr> <F|P[:tags]> <msg>` (kubelet semantics).
// It also accepts legacy docker JSON `{"log":...,"stream":...,"time":...}` lines.
func Parse(line []byte) (Line, error) {
	line = bytes.TrimSuffix(line, []byte{'\n'})
	if len(line) > 0 && line[0] == '{' {
		return parseDockerJSON(line)
	}
	i := bytes.IndexByte(line, ' ')
	if i < 0 {
		return Line{}, ErrFormat
	}
	ts, err := time.Parse(time.RFC3339Nano, string(line[:i]))
	if err != nil {
		return Line{}, ErrFormat
	}
	rest := line[i+1:]
	i = bytes.IndexByte(rest, ' ')
	if i < 0 {
		return Line{}, ErrFormat
	}
	stream := rest[:i]
	rest = rest[i+1:]
	i = bytes.IndexByte(rest, ' ')
	if i < 0 {
		// A tag with no payload ("... stdout F") is an empty line.
		if len(rest) == 0 {
			return Line{}, ErrFormat
		}
		return Line{Time: ts, Stderr: bytes.Equal(stream, []byte("stderr")), Partial: rest[0] == 'P', Msg: nil}, nil
	}
	tag := rest[:i]
	msg := rest[i+1:]
	if len(tag) == 0 {
		return Line{}, ErrFormat
	}
	return Line{Time: ts, Stderr: bytes.Equal(stream, []byte("stderr")), Partial: tag[0] == 'P', Msg: msg}, nil
}

// parseDockerJSON handles {"log":"...\n","stream":"stdout","time":"..."} without a full JSON decoder allocation.
func parseDockerJSON(line []byte) (Line, error) {
	var l Line
	logv, ok := jsonField(line, "log")
	if !ok {
		return l, ErrFormat
	}
	stream, _ := jsonField(line, "stream")
	tstr, ok := jsonField(line, "time")
	if !ok {
		return l, ErrFormat
	}
	ts, err := time.Parse(time.RFC3339Nano, tstr)
	if err != nil {
		return l, ErrFormat
	}
	msg := []byte(logv)
	partial := !bytes.HasSuffix(msg, []byte{'\n'})
	msg = bytes.TrimSuffix(msg, []byte{'\n'})
	return Line{Time: ts, Stderr: stream == "stderr", Partial: partial, Msg: msg}, nil
}

func jsonField(b []byte, key string) (string, bool) {
	k := []byte(`"` + key + `":`)
	i := bytes.Index(b, k)
	if i < 0 {
		return "", false
	}
	i += len(k)
	for i < len(b) && b[i] == ' ' {
		i++
	}
	if i >= len(b) || b[i] != '"' {
		return "", false
	}
	i++
	var out []byte
	for i < len(b) {
		c := b[i]
		if c == '"' {
			return string(out), true
		}
		if c == '\\' && i+1 < len(b) {
			i++
			switch b[i] {
			case 'n':
				out = append(out, '\n')
			case 't':
				out = append(out, '\t')
			case 'r':
				out = append(out, '\r')
			case 'u':
				if i+4 < len(b) {
					if v, err := strconv.ParseUint(string(b[i+1:i+5]), 16, 32); err == nil {
						out = append(out, []byte(string(rune(v)))...)
						i += 4
					}
				}
			default:
				out = append(out, b[i])
			}
			i++
			continue
		}
		out = append(out, c)
		i++
	}
	return "", false
}

// Merger reassembles partial (P) records into full lines, per file.
type Merger struct {
	buf     []byte
	first   time.Time
	stderr  bool
	open    bool
	MaxSize int // force-flush above this many bytes (0 = 1 MiB)
}

// Push feeds one record. It returns a completed line (ok=true) when one is ready.
// The returned Msg is owned by the caller only until the next Push.
func (m *Merger) Push(l Line) (out Line, ok bool) {
	max := m.MaxSize
	if max == 0 {
		max = 1 << 20
	}
	if !m.open {
		if !l.Partial {
			return l, true // fast path: complete line, no copy
		}
		m.open = true
		m.first = l.Time
		m.stderr = l.Stderr
		m.buf = append(m.buf[:0], l.Msg...)
		return Line{}, false
	}
	m.buf = append(m.buf, l.Msg...)
	if l.Partial && len(m.buf) < max {
		return Line{}, false
	}
	out = Line{Time: m.first, Stderr: m.stderr, Msg: m.buf}
	m.open = false
	return out, true
}

// Open reports whether a partial line is being accumulated.
func (m *Merger) Open() bool { return m.open }

// Flush returns any pending partial as a full line (used at EOF on a closed file).
func (m *Merger) Flush() (Line, bool) {
	if !m.open {
		return Line{}, false
	}
	m.open = false
	return Line{Time: m.first, Stderr: m.stderr, Msg: m.buf}, true
}
