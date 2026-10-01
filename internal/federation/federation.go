// Package federation lets one hub answer read requests on behalf of other hubs
// ("peers"): streams, query, tail, export and status fan out, and results merge by
// timestamp. Stream ids from a peer are namespaced as "<peer>/<id>".
package federation

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HdrHops guards against loops between hubs that peer with each other.
const HdrHops = "X-P10-Hops"

// MaxHops is the deepest chain of hubs a request may traverse.
const MaxHops = 3

// Peer is a remote hub.
type Peer struct {
	Name  string
	URL   string
	Token string
}

// Peers is the configured set.
type Peers struct {
	List   []Peer
	Client *http.Client
}

// New builds the peer set with a client suited to long SSE reads.
func New(list []Peer) *Peers {
	return &Peers{List: list, Client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 8, ResponseHeaderTimeout: 30 * time.Second, ForceAttemptHTTP2: true}}}
}

// Enabled reports whether any peers are configured.
func (p *Peers) Enabled() bool { return p != nil && len(p.List) > 0 }

// Find returns a peer by name.
func (p *Peers) Find(name string) (Peer, bool) {
	for _, x := range p.List {
		if x.Name == name {
			return x, true
		}
	}
	return Peer{}, false
}

// Get performs an authenticated GET on a peer, forwarding the hop count.
func (p *Peers) Get(ctx context.Context, peer Peer, path string, q url.Values, hops int, accept string) (*http.Response, error) {
	if hops+1 > MaxHops {
		return nil, errors.New("federation: too many hops")
	}
	u := strings.TrimSuffix(peer.URL, "/") + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+peer.Token)
	req.Header.Set(HdrHops, strconv.Itoa(hops+1))
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, errors.New("federation: peer " + peer.Name + " returned " + resp.Status)
	}
	return resp, nil
}

// Hops parses the incoming hop header.
func Hops(r *http.Request) int {
	n, _ := strconv.Atoi(r.Header.Get(HdrHops))
	return n
}

// SplitSID separates "<peer>/<rest>" into peer and rest; local ids have no peer.
func SplitSID(sid string) (peer, rest string) {
	if i := strings.IndexByte(sid, '/'); i > 0 {
		return sid[:i], sid[i+1:]
	}
	return "", sid
}

// Route groups sid parameters by peer ("" = local).
func Route(sids []string) map[string][]string {
	out := map[string][]string{}
	for _, s := range sids {
		for _, x := range strings.Split(s, ",") {
			if x == "" {
				continue
			}
			p, rest := SplitSID(x)
			out[p] = append(out[p], rest)
		}
	}
	return out
}
