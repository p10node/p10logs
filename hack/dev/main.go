// Development supervisor: no external watcher installation required.
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func sources() (string, error) {
	h := sha256.New()
	for _, root := range []string{"cmd/p10logs-hub", "internal", "ui", "go.mod", "go.sum", "hack/hub-dev.yaml"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, ".html") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(h, "%s\x00%x\n", path, sha256.Sum256(b))
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var child *exec.Cmd
	var done chan error
	stop := func() {
		if child == nil {
			return
		}
		_ = child.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(12 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
		child, done = nil, nil
	}
	defer stop()
	build := func() {
		log.Print("dev: building hub...")
		cmd := exec.CommandContext(ctx, "go", "build", "-o", "bin/p10logs-hub-dev", "./cmd/p10logs-hub")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			log.Print("dev: build failed; keeping the previous hub, waiting for edits")
			return
		}
		stop()
		if ctx.Err() != nil {
			return
		}
		child = exec.Command("./bin/p10logs-hub-dev", "--config", "hack/hub-dev.yaml", "--dev-ui", "ui")
		child.Env = append(os.Environ(), "P10_INGEST_TOKEN=dev")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			log.Printf("dev: start failed: %v", err)
			child = nil
			return
		}
		done = make(chan error, 1)
		go func(cmd *exec.Cmd, ch chan error) { ch <- cmd.Wait() }(child, done)
		log.Print("dev: http://127.0.0.1:8080 — Go/config: rebuild; UI: browser auto-refresh")
	}
	previous, err := sources()
	if err != nil {
		log.Fatal(err)
	}
	build()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-done:
			log.Printf("dev: hub exited (%v); check port 8080 and config, then rerun make dev", err)
			child, done = nil, nil
			return
		case <-tick.C:
			next, err := sources()
			if err != nil {
				log.Printf("dev: watching: %v", err)
				continue
			}
			if next != previous {
				previous = next
				build()
			}
		}
	}
}
