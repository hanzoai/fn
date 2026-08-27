// Command fnd is the function runtime.
//
// It is a thin server in front of the machines layer, so its whole
// configuration is where that layer is and how to prove we may talk to it.
// There is no pool size here, no replica count, no build config and no
// scheduler tuning: those knobs belong to the machines layer's Deployment,
// which is where the warm pool actually lives.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hanzoai/fn"
	"github.com/hanzoai/fn/machines"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	upstream := env("FN_MACHINES_UPSTREAM", "")
	// The credential is read from the environment and never from a file, a flag
	// or a literal: it is synced into the pod from Hanzo KMS. An empty value is
	// not an error here — a machines layer reached over a NetworkPolicy inside
	// the cluster may not want one — but it is never invented.
	key := env("FN_MACHINES_KEY", "")

	if upstream == "" {
		// Started anyway, and /healthz says 503. A process that exits on a
		// missing env var cannot tell an operator WHICH one from a CrashLoop,
		// and a process that starts and lies is worse than both.
		log.Warn("FN_MACHINES_UPSTREAM is unset; every invoke will fail closed with 503")
	}
	srv := &http.Server{
		Addr:    env("FN_ADDR", ":8000"),
		Handler: fn.NewServer(fn.NewRunner(machines.NewCloud(upstream, key), log), log).Handler(),
		// No WriteTimeout: a function may legitimately run for the full 15
		// minutes the clamp allows, and a write deadline would cut it off with
		// no answer at all. The invoke's own deadline is the real bound.
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		// Long enough for an in-flight invoke to finish and, more importantly,
		// to run its deferred release. A hard kill here leaks one machine per
		// in-flight request, every rollout.
		sctx, cancel := context.WithTimeout(context.Background(), fn.MaxTimeoutSec*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Info("fn listening", "addr", srv.Addr, "machines", upstream, "runtimes", fn.Runtimes())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
