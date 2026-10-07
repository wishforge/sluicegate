package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/wishforge/sluicegate/internal/agent"
	"github.com/wishforge/sluicegate/internal/common"
)

func main() {
	log := common.NewLogger("source-agent")
	metrics := &common.Metrics{}
	root := getenv("WORKLOAD_ROOT", "./source-workloads")
	checkpoint := getenv("CHECKPOINT_ROOT", "./source-checkpoints")
	s, err := agent.NewSourceServer(root, checkpoint, log, metrics)
	if err != nil {
		log.Error("startup_failed", "error", err.Error())
		os.Exit(1)
	}
	srv := common.NewHTTPServer(getenv("HTTP_ADDR", ":8081"), s.Handler())
	go func() {
		log.Info("server_started", "addr", srv.Addr, "role", "source-agent")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server_stopped_with_error", "error", err.Error())
			os.Exit(1)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	common.Shutdown(srv)
}
func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
