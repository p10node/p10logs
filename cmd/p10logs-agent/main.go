// p10logs-agent tails /var/log/pods on a node and pushes lines to a p10logs hub.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/p10node/p10logs/internal/agent"
)

func main() {
	cfgPath := flag.String("config", "", "path to agent.yaml")
	flag.Parse()
	cfg, err := agent.Load(*cfgPath)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	lvl := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := agent.Run(ctx, cfg, log); err != nil {
		log.Error("agent", "err", err)
		os.Exit(1)
	}
}
