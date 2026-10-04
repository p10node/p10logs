package agent

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/p10node/p10logs/internal/enrich"
	"github.com/p10node/p10logs/internal/ship"
	"github.com/p10node/p10logs/internal/tail"
)

// Version is set at build time.
var Version = "dev"

// Run starts the agent and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg *Config, log *slog.Logger) error {
	node := os.Getenv("NODE_NAME")
	if node == "" {
		node, _ = os.Hostname()
	}
	if err := os.MkdirAll(cfg.Paths.State, 0o755); err != nil {
		return err
	}
	pos, err := tail.LoadPositions(filepath.Join(cfg.Paths.State, "positions.json"))
	if err != nil {
		return err
	}
	client, err := ship.NewClient(ship.ClientConfig{URL: cfg.Hub.URL, TokenFile: cfg.Hub.TokenFile, Token: cfg.Hub.Token,
		InsecureSkipVerify: cfg.Hub.TLS.InsecureSkipVerify, CAFile: cfg.Hub.TLS.CAFile, Cluster: cfg.Cluster, Node: node, Version: Version})
	if err != nil {
		return err
	}
	client.Log = log
	spool := &ship.Spool{Dir: filepath.Join(cfg.Paths.State, "buffer"), MaxBytes: cfg.Buffer.MaxBytes}
	if err := spool.Open(); err != nil {
		return err
	}
	batcher := ship.NewBatcher(ship.BatcherConfig{MaxBytes: cfg.Batch.MaxBytes, MaxLines: cfg.Batch.MaxLines, FlushInterval: cfg.Batch.FlushInterval,
		LinesPerSec: cfg.RateLimit.LinesPerSecondPerContainer, Multiline: cfg.Multiline.Enabled, StartPattern: cfg.Multiline.StartPattern,
		MultiMaxLines: cfg.Multiline.MaxLines, MultiTimeout: cfg.Multiline.Timeout}, cfg.Ship.MaxInFlight)
	shipper := &ship.Shipper{Client: client, Spool: spool, Log: log, OnAck: func(acks []ship.Ack) {
		for _, a := range acks {
			p, _ := pos.Get(a.Key)
			if a.EndOff >= p.Offset {
				p.Offset = a.EndOff
			}
			pos.Set(a.Key, p)
		}
	}}
	disc := &tail.Discoverer{Dir: cfg.Paths.Pods, Positions: pos, Sink: batcher, Backfill: cfg.Backfill.Compressed, Log: log,
		Filter: tail.Filter{IncludeNS: cfg.Collect.IncludeNamespaces, ExcludeNS: cfg.Collect.ExcludeNamespaces, ExcludeCtrs: cfg.Collect.ExcludeContainers,
			SelfNS: os.Getenv("POD_NAMESPACE"), SelfPod: os.Getenv("POD_NAME"), CollectSelf: cfg.Collect.Self}}
	if cfg.Enrich.Enabled {
		w, err := enrich.New(enrich.Config{Node: node, Labels: cfg.Enrich.Labels, Annotations: cfg.Enrich.Annotations, Log: log})
		if err != nil {
			log.Warn("enrich disabled", "err", err)
		} else {
			batcher.LabelsFor = w.Lookup
			go w.Run(ctx)
			log.Info("enrich enabled", "labels", cfg.Enrich.Labels, "annotations", cfg.Enrich.Annotations)
		}
	}

	// health endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "files=%d lag_bytes=%d spool_bytes=%d dropped=%d sent=%d failed=%d\n", disc.Active(), disc.Lag(), spool.Size(), batcher.Dropped+spool.Dropped.Load(), shipper.Sent, shipper.Failed)
	})
	hs := &http.Server{Addr: cfg.Health, Handler: mux}
	go hs.ListenAndServe()

	var wg sync.WaitGroup
	bctx, bcancel := context.WithCancel(context.Background()) // batcher outlives discovery so the final flush ships
	wg.Add(3)
	go func() { defer wg.Done(); disc.Run(ctx) }()
	go func() { defer wg.Done(); batcher.Run(bctx) }()
	go func() { defer wg.Done(); shipper.Run(bctx, batcher.Out()) }()

	// heartbeat + checkpoint save
	hb := time.NewTicker(cfg.HeartbeatInterval)
	sv := time.NewTicker(time.Second)
	hbFails := 0
	defer hb.Stop()
	defer sv.Stop()
	log.Info("p10logs-agent started", "version", Version, "cluster", cfg.Cluster, "node", node, "hub", cfg.Hub.URL, "pods", cfg.Paths.Pods)
	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down: flushing")
			bcancel()
			wg.Wait()
			pos.Save()
			hs.Close()
			return nil
		case <-sv.C:
			if err := pos.Save(); err != nil {
				log.Error("positions save", "err", err)
			}
		case <-hb.C:
			stats := fmt.Sprintf("files=%d;lag_bytes=%d;spool_bytes=%d;dropped=%d", disc.Active(), disc.Lag(), spool.Size(), batcher.Dropped+spool.Dropped.Load())
			hctx, c := context.WithTimeout(ctx, 5*time.Second)
			if err := client.Push(hctx, nil, stats); err != nil {
				if hbFails%30 == 0 { // first failure, then every ~5 minutes at 10s interval
					log.Warn("heartbeat to hub failed", "err", err, "consecutive", hbFails+1)
				}
				hbFails++
			} else if hbFails > 0 {
				log.Info("hub reachable again", "after_failures", hbFails)
				hbFails = 0
			}
			c()
		}
	}
}
