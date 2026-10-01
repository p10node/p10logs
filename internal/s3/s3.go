// Package s3 is a minimal S3-compatible client (SigV4, path-style or virtual-host)
// with just what the hub needs: put, get (with range), delete, list. No SDK.
package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config for a bucket.
type Config struct {
	Endpoint  string // https://s3.amazonaws.com or http://minio:9000
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	PathStyle bool
	Prefix    string // key prefix inside the bucket
}

// Client talks to one bucket.
type Client struct {
	cfg  Config
	http *http.Client
}

// New builds a client.
func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, errors.New("s3: endpoint and bucket are required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	cfg.Endpoint = strings.TrimSuffix(cfg.Endpoint, "/")
	return &Client{cfg: cfg, http: &http.Client{Timeout: 5 * time.Minute}}, nil
}

// ErrNotFound is returned for a missing object.
var ErrNotFound = errors.New("s3: not found")

func (c *Client) objURL(key string) (string, string) {
	u, _ := url.Parse(c.cfg.Endpoint)
	path := "/" + c.cfg.Bucket + "/" + key
	if !c.cfg.PathStyle {
		u.Host = c.cfg.Bucket + "." + u.Host
		path = "/" + key
	}
	return u.Scheme + "://" + u.Host, path
}

func (c *Client) key(k string) string {
	if c.cfg.Prefix == "" {
		return k
	}
	return strings.TrimSuffix(c.cfg.Prefix, "/") + "/" + k
}

// sign adds SigV4 headers (payload hash of body must be given).
func (c *Client) sign(req *http.Request, payloadHash string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("host", req.URL.Host)
	var hdrs []string
	for k := range req.Header {
		lk := strings.ToLower(k)
		if lk == "host" || strings.HasPrefix(lk, "x-amz-") || lk == "content-type" || lk == "range" {
			hdrs = append(hdrs, lk)
		}
	}
	sort.Strings(hdrs)
	var canonHdrs strings.Builder
	for _, h := range hdrs {
		v := req.Header.Get(h)
		if h == "host" {
			v = req.URL.Host
		}
		canonHdrs.WriteString(h + ":" + strings.TrimSpace(v) + "\n")
	}
	signed := strings.Join(hdrs, ";")
	// canonical query
	q := req.URL.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var cq []string
	for _, k := range keys {
		for _, v := range q[k] {
			cq = append(cq, url.QueryEscape(k)+"="+strings.ReplaceAll(url.QueryEscape(v), "+", "%20"))
		}
	}
	canonPath := req.URL.EscapedPath()
	canon := strings.Join([]string{req.Method, canonPath, strings.Join(cq, "&"), canonHdrs.String(), signed, payloadHash}, "\n")
	scope := date + "/" + c.cfg.Region + "/s3/aws4_request"
	sts := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA([]byte(canon))
	k := hmacSHA([]byte("AWS4"+c.cfg.SecretKey), []byte(date))
	k = hmacSHA(k, []byte(c.cfg.Region))
	k = hmacSHA(k, []byte("s3"))
	k = hmacSHA(k, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA(k, []byte(sts)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.cfg.AccessKey+"/"+scope+", SignedHeaders="+signed+", Signature="+sig)
}

func hexSHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func hmacSHA(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func (c *Client) do(ctx context.Context, method, key string, query url.Values, body []byte, extra http.Header) (*http.Response, error) {
	base, path := c.objURL(key)
	u := base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rdr io.Reader
	if body != nil {
		rdr = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, err
	}
	for k, v := range extra {
		req.Header[k] = v
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	payloadHash := hexSHA(body)
	c.sign(req, payloadHash)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 404 {
		resp.Body.Close()
		return nil, ErrNotFound
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, fmt.Errorf("s3: %s %s: %s %s", method, key, resp.Status, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

// EnsureBucket creates the bucket if it does not exist (MinIO, R2 and S3 accept a
// plain PUT on the bucket; "already owned" counts as success).
func (c *Client) EnsureBucket(ctx context.Context) error {
	base, path := c.objURL("")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base+strings.TrimSuffix(path, "/"), nil)
	if err != nil {
		return err
	}
	c.sign(req, hexSHA(nil))
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 || resp.StatusCode == 409 {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("s3: create bucket: %s %s", resp.Status, strings.TrimSpace(string(b)))
}

// Put uploads an object.
func (c *Client) Put(ctx context.Context, key string, body []byte) error {
	resp, err := c.do(ctx, http.MethodPut, c.key(key), nil, body, http.Header{"Content-Type": {"application/octet-stream"}})
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Get downloads an object, optionally a byte range [from, to] (to<0 = to end).
func (c *Client) Get(ctx context.Context, key string, from, to int64) ([]byte, error) {
	h := http.Header{}
	if from > 0 || to >= 0 {
		if to < 0 {
			h.Set("Range", "bytes="+strconv.FormatInt(from, 10)+"-")
		} else {
			h.Set("Range", "bytes="+strconv.FormatInt(from, 10)+"-"+strconv.FormatInt(to, 10))
		}
	}
	resp, err := c.do(ctx, http.MethodGet, c.key(key), nil, nil, h)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// Delete removes an object (missing = ok).
func (c *Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, c.key(key), nil, nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Object is a listing entry.
type Object struct {
	Key  string
	Size int64
}

// List returns all objects under prefix (relative to the client's prefix).
func (c *Client) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {c.key(prefix)}, "max-keys": {"1000"}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := c.do(ctx, http.MethodGet, "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		var lr struct {
			Contents []struct {
				Key  string `xml:"Key"`
				Size int64  `xml:"Size"`
			} `xml:"Contents"`
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
		}
		err = xml.NewDecoder(resp.Body).Decode(&lr)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		pre := ""
		if c.cfg.Prefix != "" {
			pre = strings.TrimSuffix(c.cfg.Prefix, "/") + "/"
		}
		for _, o := range lr.Contents {
			out = append(out, Object{Key: strings.TrimPrefix(o.Key, pre), Size: o.Size})
		}
		if !lr.IsTruncated || lr.NextContinuationToken == "" {
			return out, nil
		}
		token = lr.NextContinuationToken
	}
}
