// Package index is the hub's SQLite catalog of streams, sealed chunks and agent cursors.
// It is derived state: it can be rebuilt from the chunk files at any time.
package index

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Stream is one (cluster, ns, pod, uid, container).
type Stream struct {
	ID        int64             `json:"id"`
	Cluster   string            `json:"cluster"`
	Namespace string            `json:"namespace"`
	Pod       string            `json:"pod"`
	UID       string            `json:"uid"`
	Container string            `json:"container"`
	Node      string            `json:"node,omitempty"`
	Restarts  int               `json:"restarts"`
	FirstTS   int64             `json:"first_ts"`
	LastTS    int64             `json:"last_ts"`
	Ended     bool              `json:"ended"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Chunk is a sealed chunk file.
type Chunk struct {
	ID       int64
	StreamID int64
	Path     string
	Day      string
	MinTS    int64
	MaxTS    int64
	Lines    int
	Bytes    int64
	Remote   bool // copy exists in object storage
	Local    bool // file exists on the local disk
}

// Selector filters streams; fields accept "*" globs.
type Selector struct {
	Cluster, Namespace, Pod, Container string
	UID                                string // exact pod uid
	IDs                                []int64
	Since                              int64
	Labels                             map[string]string // exact match on enrichment labels
	// Allow, when set, drops streams the caller may not read (viewer roles).
	Allow func(cluster, namespace string) bool
}

// DB wraps the SQLite connection.
type DB struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS streams(
  id INTEGER PRIMARY KEY, cluster TEXT, ns TEXT, pod TEXT, uid TEXT, ctr TEXT, node TEXT,
  restarts INTEGER DEFAULT 0, first_ts INTEGER, last_ts INTEGER, ended INTEGER DEFAULT 0,
  UNIQUE(cluster, ns, pod, uid, ctr));
CREATE INDEX IF NOT EXISTS streams_lookup ON streams(cluster, ns, pod);
CREATE TABLE IF NOT EXISTS chunks(
  id INTEGER PRIMARY KEY, stream_id INTEGER, path TEXT UNIQUE, day TEXT,
  min_ts INTEGER, max_ts INTEGER, lines INTEGER, bytes INTEGER);
CREATE INDEX IF NOT EXISTS chunks_stream ON chunks(stream_id, max_ts);
CREATE INDEX IF NOT EXISTS chunks_day ON chunks(day);
CREATE TABLE IF NOT EXISTS cursors(stream_id INTEGER, file TEXT, end_off INTEGER, PRIMARY KEY(stream_id, file));
`

// Open opens or creates the database.
func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // serialise writers; reads are fast enough
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	db.Exec(`ALTER TABLE streams ADD COLUMN labels TEXT`)             // v0.2 migration; error = column exists
	db.Exec(`ALTER TABLE streams ADD COLUMN wseq INTEGER DEFAULT 0`)  // v0.3 migration
	db.Exec(`ALTER TABLE chunks ADD COLUMN remote INTEGER DEFAULT 0`) // v1.0: object-storage offload
	db.Exec(`ALTER TABLE chunks ADD COLUMN local INTEGER DEFAULT 1`)
	return &DB{db: db}, nil
}

// Close closes the database.
func (d *DB) Close() error { return d.db.Close() }

// Reset drops all rows (used by --rebuild-index).
func (d *DB) Reset() error {
	_, err := d.db.Exec("DELETE FROM chunks; DELETE FROM cursors; DELETE FROM streams;")
	return err
}

// UpsertStream returns the stream id, creating the row if needed and bumping last_ts/restarts/ended.
func (d *DB) UpsertStream(s Stream) (int64, error) {
	ended := 0
	if s.Ended {
		ended = 1
	}
	var labels any
	if len(s.Labels) > 0 {
		b, _ := json.Marshal(s.Labels)
		labels = string(b)
	}
	var id int64
	err := d.db.QueryRow(`INSERT INTO streams(cluster,ns,pod,uid,ctr,node,restarts,first_ts,last_ts,ended,labels)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(cluster,ns,pod,uid,ctr) DO UPDATE SET
		  node=CASE WHEN excluded.node<>'' THEN excluded.node ELSE node END, restarts=max(restarts,excluded.restarts),
		  first_ts=CASE WHEN first_ts=0 OR (excluded.first_ts>0 AND excluded.first_ts<first_ts) THEN excluded.first_ts ELSE first_ts END,
		  last_ts=max(last_ts,excluded.last_ts), ended=max(ended,excluded.ended),
		  labels=COALESCE(excluded.labels, labels)
		RETURNING id`, s.Cluster, s.Namespace, s.Pod, s.UID, s.Container, s.Node, s.Restarts, s.FirstTS, s.LastTS, ended, labels).Scan(&id)
	return id, err
}

// SetWSeq records the highest WAL sequence flushed to a chunk for a stream.
func (d *DB) SetWSeq(streamID int64, seq uint64) error {
	_, err := d.db.Exec(`UPDATE streams SET wseq=max(wseq,?) WHERE id=?`, int64(seq), streamID)
	return err
}

// WSeqs returns the flushed WAL sequence per stream.
func (d *DB) WSeqs() (map[int64]uint64, error) {
	rows, err := d.db.Query(`SELECT id, COALESCE(wseq,0) FROM streams`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]uint64{}
	for rows.Next() {
		var id, w int64
		if err := rows.Scan(&id, &w); err != nil {
			return nil, err
		}
		out[id] = uint64(w)
	}
	return out, rows.Err()
}

// GetCursor returns the acknowledged end offset for (stream, file).
func (d *DB) GetCursor(streamID int64, file string) int64 {
	var off int64
	_ = d.db.QueryRow(`SELECT end_off FROM cursors WHERE stream_id=? AND file=?`, streamID, file).Scan(&off)
	return off
}

// SetCursor stores the end offset.
func (d *DB) SetCursor(streamID int64, file string, end int64) error {
	_, err := d.db.Exec(`INSERT INTO cursors(stream_id,file,end_off) VALUES(?,?,?) ON CONFLICT(stream_id,file) DO UPDATE SET end_off=max(end_off,excluded.end_off)`, streamID, file, end)
	return err
}

// InsertChunk records a sealed chunk (Local defaults to true unless Remote-only).
func (d *DB) InsertChunk(c Chunk) error {
	local := 1
	if c.Remote && !c.Local {
		local = 0
	}
	remote := 0
	if c.Remote {
		remote = 1
	}
	_, err := d.db.Exec(`INSERT OR REPLACE INTO chunks(stream_id,path,day,min_ts,max_ts,lines,bytes,remote,local) VALUES(?,?,?,?,?,?,?,?,?)`,
		c.StreamID, c.Path, c.Day, c.MinTS, c.MaxTS, c.Lines, c.Bytes, remote, local)
	return err
}

// MarkRemote flags a chunk as copied to object storage.
func (d *DB) MarkRemote(id int64) error {
	_, err := d.db.Exec(`UPDATE chunks SET remote=1 WHERE id=?`, id)
	return err
}

// MarkLocal records whether the local file still exists.
func (d *DB) MarkLocal(id int64, local bool) error {
	v := 0
	if local {
		v = 1
	}
	_, err := d.db.Exec(`UPDATE chunks SET local=? WHERE id=?`, v, id)
	return err
}

func (d *DB) scanChunks(rows *sql.Rows) ([]Chunk, error) {
	defer rows.Close()
	var out []Chunk
	for rows.Next() {
		var c Chunk
		var remote, local int
		if err := rows.Scan(&c.ID, &c.StreamID, &c.Path, &c.Day, &c.MinTS, &c.MaxTS, &c.Lines, &c.Bytes, &remote, &local); err != nil {
			return nil, err
		}
		c.Remote, c.Local = remote == 1, local == 1
		out = append(out, c)
	}
	return out, rows.Err()
}

const chunkCols = `id,stream_id,path,day,min_ts,max_ts,lines,bytes,COALESCE(remote,0),COALESCE(local,1)`

// OffloadCandidates lists local-only chunks whose newest line is older than cutoff.
func (d *DB) OffloadCandidates(cutoff int64, limit int) ([]Chunk, error) {
	rows, err := d.db.Query(`SELECT `+chunkCols+` FROM chunks WHERE COALESCE(remote,0)=0 AND max_ts<? ORDER BY max_ts LIMIT ?`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	return d.scanChunks(rows)
}

// EvictCandidates lists chunks that have a remote copy and still occupy local disk, oldest first.
func (d *DB) EvictCandidates(limit int) ([]Chunk, error) {
	rows, err := d.db.Query(`SELECT `+chunkCols+` FROM chunks WHERE COALESCE(remote,0)=1 AND COALESCE(local,1)=1 ORDER BY max_ts LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	return d.scanChunks(rows)
}

// ChunksByPrefix lists chunks under a path prefix (used to delete remote copies).
func (d *DB) ChunksByPrefix(prefix string) ([]Chunk, error) {
	rows, err := d.db.Query(`SELECT `+chunkCols+` FROM chunks WHERE path LIKE ? ESCAPE '\'`, strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)+"%")
	if err != nil {
		return nil, err
	}
	return d.scanChunks(rows)
}

func globToLike(g string) (string, bool) {
	if g == "" || g == "*" {
		return "", false
	}
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return strings.ReplaceAll(r.Replace(g), "*", "%"), true
}

func (s Selector) where() (string, []any) {
	var w []string
	var args []any
	for col, v := range map[string]string{"cluster": s.Cluster, "ns": s.Namespace, "pod": s.Pod, "ctr": s.Container} {
		if l, ok := globToLike(v); ok {
			if strings.ContainsAny(v, "*") {
				w = append(w, col+` LIKE ? ESCAPE '\'`)
			} else {
				w = append(w, col+`=?`)
				l = v
			}
			args = append(args, l)
		}
	}
	if s.UID != "" {
		w = append(w, "uid=?")
		args = append(args, s.UID)
	}
	if len(s.IDs) > 0 {
		q := make([]string, len(s.IDs))
		for i, id := range s.IDs {
			q[i] = "?"
			args = append(args, id)
		}
		w = append(w, "id IN ("+strings.Join(q, ",")+")")
	}
	if s.Since > 0 {
		w = append(w, "last_ts>=?")
		args = append(args, s.Since)
	}
	for k, v := range s.Labels {
		w = append(w, `json_extract(labels, ?)=?`)
		args = append(args, `$."`+strings.ReplaceAll(k, `"`, "")+`"`, v)
	}
	if len(w) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(w, " AND "), args
}

// ListStreams returns streams matching the selector, newest activity first.
func (d *DB) ListStreams(ctx context.Context, s Selector, limit int) ([]Stream, error) {
	w, args := s.where()
	if limit <= 0 {
		limit = 100000
	}
	rows, err := d.db.QueryContext(ctx, `SELECT id,cluster,ns,pod,uid,ctr,node,restarts,first_ts,last_ts,ended,COALESCE(labels,'') FROM streams`+w+` ORDER BY last_ts DESC LIMIT `+itoa(limit), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Stream
	for rows.Next() {
		var st Stream
		var ended int
		var labels string
		if err := rows.Scan(&st.ID, &st.Cluster, &st.Namespace, &st.Pod, &st.UID, &st.Container, &st.Node, &st.Restarts, &st.FirstTS, &st.LastTS, &ended, &labels); err != nil {
			return nil, err
		}
		st.Ended = ended == 1
		if labels != "" {
			json.Unmarshal([]byte(labels), &st.Labels)
		}
		if s.Allow != nil && !s.Allow(st.Cluster, st.Namespace) {
			continue
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// ChunksFor returns sealed chunks of the given streams overlapping [start,end].
func (d *DB) ChunksFor(ctx context.Context, ids []int64, start, end int64) ([]Chunk, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	q := make([]string, len(ids))
	args := make([]any, 0, len(ids)+2)
	for i, id := range ids {
		q[i] = "?"
		args = append(args, id)
	}
	args = append(args, end, start)
	rows, err := d.db.QueryContext(ctx, `SELECT `+chunkCols+` FROM chunks WHERE stream_id IN (`+strings.Join(q, ",")+`) AND min_ts<=? AND max_ts>=? ORDER BY max_ts DESC`, args...)
	if err != nil {
		return nil, err
	}
	return d.scanChunks(rows)
}

// Stats used by retention and status.
type Stats struct {
	Streams, Live, Chunks int64
	Bytes                 int64 // local bytes
	RemoteChunks          int64
	RemoteOnlyChunks      int64
	Days                  []string // ascending
	DayBytes              map[string]int64
}

// Stats summarises the catalog.
func (d *DB) Stats() (Stats, error) {
	var s Stats
	s.DayBytes = map[string]int64{}
	if err := d.db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN ended=0 THEN 1 ELSE 0 END),0) FROM streams`).Scan(&s.Streams, &s.Live); err != nil {
		return s, err
	}
	if err := d.db.QueryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN COALESCE(local,1)=1 THEN bytes ELSE 0 END),0),
		COALESCE(SUM(COALESCE(remote,0)),0), COALESCE(SUM(CASE WHEN COALESCE(remote,0)=1 AND COALESCE(local,1)=0 THEN 1 ELSE 0 END),0) FROM chunks`).Scan(&s.Chunks, &s.Bytes, &s.RemoteChunks, &s.RemoteOnlyChunks); err != nil {
		return s, err
	}
	rows, err := d.db.Query(`SELECT day, SUM(CASE WHEN COALESCE(local,1)=1 THEN bytes ELSE 0 END) FROM chunks GROUP BY day ORDER BY day`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var day string
		var b int64
		rows.Scan(&day, &b)
		s.Days = append(s.Days, day)
		s.DayBytes[day] = b
	}
	return s, nil
}

// DeleteChunksByPrefix removes chunk rows whose path starts with prefix.
func (d *DB) DeleteChunksByPrefix(prefix string) (int64, error) {
	res, err := d.db.Exec(`DELETE FROM chunks WHERE path LIKE ? ESCAPE '\'`, strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)+"%")
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteOrphanStreams removes streams with no chunks and no activity since cutoff.
func (d *DB) DeleteOrphanStreams(cutoff int64) (int64, error) {
	res, err := d.db.Exec(`DELETE FROM streams WHERE last_ts<? AND id NOT IN (SELECT DISTINCT stream_id FROM chunks)`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		d.db.Exec(`DELETE FROM cursors WHERE stream_id NOT IN (SELECT id FROM streams)`)
	}
	return n, nil
}

// MarkEnded flags streams without activity since cutoff as ended (agent died, pod gone).
func (d *DB) MarkEnded(streamID int64) error {
	_, err := d.db.Exec(`UPDATE streams SET ended=1 WHERE id=?`, streamID)
	return err
}

func itoa(n int) string {
	if n <= 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// ErrNotFound is returned for a missing row.
var ErrNotFound = errors.New("index: not found")

// Day formats a nanosecond timestamp as a UTC day partition name.
func Day(tsNanos int64) string { return time.Unix(0, tsNanos).UTC().Format("20060102") }
