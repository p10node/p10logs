// p10logs-hub receives, stores, indexes and serves pod logs.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/p10node/p10logs/internal/api"
	"github.com/p10node/p10logs/internal/auth"
	"github.com/p10node/p10logs/internal/federation"
	"github.com/p10node/p10logs/internal/hub"
	"github.com/p10node/p10logs/internal/s3"
	"github.com/p10node/p10logs/internal/store"
	"github.com/p10node/p10logs/ui"
)

// Version is set at build time.
var Version = "dev"

func main() {
	cfgPath := flag.String("config", "", "path to hub.yaml")
	rebuild := flag.Bool("rebuild-index", false, "recreate index.sqlite from chunk files (and object storage) and exit")
	check := flag.Bool("check", false, "verify chunks, WAL and index; print a JSON report and exit (non-zero if problems)")
	flag.Parse()
	cfg, err := hub.Load(*cfgPath)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	lvl := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	sc := store.Config{Dir: cfg.DataDir, TargetBytes: cfg.Storage.Segment.TargetBytes, MaxOpenAge: cfg.Storage.Segment.MaxOpenAge,
		FlushBytes: cfg.Storage.Memtable.FlushBytes, FlushAge: cfg.Storage.Memtable.FlushAge, MemMaxBytes: cfg.Storage.Memtable.MaxBytes,
		WALSegmentBytes: cfg.Storage.WAL.SegmentBytes,
		MaxAge:          cfg.Storage.Retention.MaxAge, MaxDiskBytes: cfg.Storage.Retention.MaxDiskBytes,
		MaxScanBytes: cfg.Query.MaxScanBytes, QueryTimeout: cfg.Query.Timeout, MaxLineBytes: cfg.Limits.MaxLineBytes}
	for _, o := range cfg.Storage.Retention.Overrides {
		sc.Overrides = append(sc.Overrides, store.Override{Match: o.Match, MaxAge: o.MaxAge})
	}
	if ob := cfg.Storage.ObjectStore; ob.Enabled {
		obj, err := s3.New(s3.Config{Endpoint: ob.Endpoint, Bucket: ob.Bucket, Region: ob.Region, AccessKey: ob.AccessKey, SecretKey: ob.SecretKey, PathStyle: ob.PathStyle, Prefix: ob.Prefix})
		if err != nil {
			log.Error("objectStore", "err", err)
			os.Exit(1)
		}
		sc.Object, sc.UploadAfter, sc.CacheBytes, sc.OffloadEvery = obj, ob.UploadAfter, ob.CacheBytes, ob.Interval
		bctx, bcancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := obj.EnsureBucket(bctx); err != nil {
			log.Warn("objectStore: bucket not ready (will retry on first upload)", "bucket", ob.Bucket, "err", err)
		}
		bcancel()
	}
	st, err := store.Open(sc, log)
	if err != nil {
		log.Error("store", "err", err)
		os.Exit(1)
	}
	if *rebuild {
		if err := st.RebuildIndex(); err != nil {
			log.Error("rebuild", "err", err)
			os.Exit(1)
		}
		st.Close()
		return
	}
	if *check {
		rep := st.Check()
		json.NewEncoder(os.Stdout).Encode(rep)
		st.Idx.Close()
		if !rep.OK {
			os.Exit(2)
		}
		return
	}
	var peers *federation.Peers
	if cfg.Federation.Enabled && len(cfg.Federation.Peers) > 0 {
		var list []federation.Peer
		for _, p := range cfg.Federation.Peers {
			if p.Token == "" {
				log.Warn("federation peer has no token; skipping", "peer", p.Name)
				continue
			}
			list = append(list, federation.Peer{Name: p.Name, URL: p.URL, Token: p.Token})
		}
		peers = federation.New(list)
		log.Info("federation enabled", "peers", len(list))
	}
	if len(cfg.Auth.IngestTokens) == 0 && len(cfg.Auth.ClusterTokens) == 0 {
		log.Error("no ingest token configured (auth.ingestToken / ingestTokenFile / clusterTokens / P10_INGEST_TOKEN)")
		os.Exit(1)
	}
	var clusterTokens []auth.ClusterToken
	for _, ct := range cfg.Auth.ClusterTokens {
		if ct.Token != "" {
			clusterTokens = append(clusterTokens, auth.ClusterToken{Token: ct.Token, Clusters: ct.Clusters})
		}
	}
	var roles []auth.Role
	for _, r := range cfg.Auth.Roles {
		roles = append(roles, auth.Role{Name: r.Name, Users: r.Users, Domains: r.Domains, APITokens: r.APITokens, Clusters: r.Clusters, Namespaces: r.Namespaces})
	}
	sessionKey, _ := os.ReadFile(cfg.Auth.SessionKeyFile)
	if len(sessionKey) < 32 { // generate once so sessions survive restarts
		sessionKey = make([]byte, 32)
		rand.Read(sessionKey)
		if err := os.MkdirAll(filepath.Dir(cfg.Auth.SessionKeyFile), 0o700); err == nil {
			os.WriteFile(cfg.Auth.SessionKeyFile, sessionKey, 0o600)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a, err := auth.New(ctx, auth.Config{IngestTokens: cfg.Auth.IngestTokens, ClusterTokens: clusterTokens, APITokens: cfg.Auth.APITokens, Roles: roles, Mode: cfg.Auth.UI.Mode,
		BasicUser: cfg.Auth.UI.Basic.Username, BasicPass: cfg.Auth.UI.Basic.Password, LockPassword: cfg.Auth.UI.Basic.LockPassword,
		OIDCIssuer: cfg.Auth.UI.OIDC.IssuerURL, OIDCClientID: cfg.Auth.UI.OIDC.ClientID, OIDCSecret: cfg.Auth.UI.OIDC.ClientSecret,
		AllowedEmails: cfg.Auth.UI.OIDC.AllowedEmails, AllowedDomains: cfg.Auth.UI.OIDC.AllowedDomains, SessionKey: sessionKey, PublicURL: cfg.Auth.PublicURL,
		UsersFile: cfg.Auth.UsersFile})
	if err != nil {
		log.Error("auth", "err", err)
		os.Exit(1)
	}
	srv := api.New(api.Config{MaxBatchBytes: cfg.Limits.MaxBatchBytes, PerClusterBPS: cfg.Limits.PerCluster.BytesPerSecond, MaxConcurrent: cfg.Query.MaxConcurrent,
		TailMaxClients: cfg.Query.Tail.MaxClients, TailLinesPerSec: cfg.Query.Tail.MaxLinesPerSecondPerClient, TailBuffer: cfg.Query.Tail.BufferLines,
		MetricsEnabled: cfg.Metrics.Enabled, Version: Version, Peers: peers}, st, a, ui.Index)
	hs := &http.Server{Addr: cfg.Listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go st.Run(ctx)
	go func() {
		log.Info("p10logs-hub listening", "addr", cfg.Listen, "data", cfg.DataDir, "ui_auth", a.Mode(), "version", Version)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs.Shutdown(sctx)
	st.Close()
}
