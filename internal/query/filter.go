// Package query implements the grep-like filter grammar shared by the API and UI:
//
//	word        case-insensitive substring
//	!word       negated
//	"a phrase"  exact phrase, case-insensitive
//	/re2/       regular expression
//	key=value   JSON lines whose top-level key equals value (case-insensitive)
//
// All terms must match.
package query

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

type termKind int

const (
	kWord termKind = iota
	kPhrase
	kRegex
	kKV
)

type term struct {
	kind termKind
	neg  bool
	val  string
	key  string
	re   *regexp.Regexp
}

// Filter is a compiled query.
type Filter struct {
	terms []term
}

var tokRe = regexp.MustCompile(`"([^"]*)"|/((?:\\/|[^/])+)/|(\S+)`)

// Compile parses q. Invalid regexes are ignored (treated as no-op) so a half-typed
// pattern never breaks live tail.
func Compile(q string) *Filter {
	f := &Filter{}
	for _, m := range tokRe.FindAllStringSubmatch(q, -1) {
		switch {
		case strings.HasPrefix(m[0], `"`):
			if m[1] != "" {
				f.terms = append(f.terms, term{kind: kPhrase, val: strings.ToLower(m[1])})
			}
		case m[2] != "":
			if re, err := regexp.Compile("(?i)" + strings.ReplaceAll(m[2], `\/`, "/")); err == nil {
				f.terms = append(f.terms, term{kind: kRegex, re: re})
			}
		default:
			w := m[3]
			neg := false
			if strings.HasPrefix(w, "!") {
				neg = true
				w = w[1:]
			}
			if w == "" || strings.HasPrefix(w, "/") { // empty, or a regex still being typed
				continue
			}
			if i := strings.IndexByte(w, '='); i > 0 && isKey(w[:i]) {
				f.terms = append(f.terms, term{kind: kKV, neg: neg, key: w[:i], val: strings.ToLower(w[i+1:])})
			} else {
				f.terms = append(f.terms, term{kind: kWord, neg: neg, val: strings.ToLower(w)})
			}
		}
	}
	return f
}

func isKey(s string) bool {
	for i, c := range s {
		if !(c == '_' || c == '.' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || (i > 0 && c >= '0' && c <= '9')) {
			return false
		}
	}
	return s != ""
}

// Empty reports whether the filter matches everything.
func (f *Filter) Empty() bool { return f == nil || len(f.terms) == 0 }

// Words returns positive literal terms (for highlighting / bloom pre-filtering).
func (f *Filter) Words() []string {
	var out []string
	for _, t := range f.terms {
		if (t.kind == kWord || t.kind == kPhrase) && !t.neg {
			out = append(out, t.val)
		}
	}
	return out
}

// Match reports whether msg satisfies all terms.
func (f *Filter) Match(msg []byte) bool {
	if f.Empty() {
		return true
	}
	var low []byte
	var obj map[string]json.RawMessage
	objDone := false
	for _, t := range f.terms {
		var ok bool
		switch t.kind {
		case kWord, kPhrase:
			if low == nil {
				low = bytes.ToLower(msg)
			}
			ok = bytes.Contains(low, []byte(t.val))
		case kRegex:
			ok = t.re.Match(msg)
		case kKV:
			if !objDone {
				objDone = true
				if len(msg) > 0 && msg[0] == '{' {
					_ = json.Unmarshal(msg, &obj)
				}
			}
			if obj != nil {
				if raw, has := obj[t.key]; has {
					var s string
					if err := json.Unmarshal(raw, &s); err != nil {
						s = string(raw)
					}
					ok = strings.ToLower(s) == t.val
				}
			}
		}
		if t.neg {
			ok = !ok
		}
		if !ok {
			return false
		}
	}
	return true
}
