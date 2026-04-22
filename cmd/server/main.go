package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danharper32/artifact-registry-service/internal/api"
	"github.com/danharper32/artifact-registry-service/internal/config"
	"github.com/danharper32/artifact-registry-service/internal/repository/sqlite"
	"github.com/danharper32/artifact-registry-service/internal/service"
	"github.com/danharper32/artifact-registry-service/internal/storage/local"
	"github.com/danharper32/artifact-registry-service/internal/webhooks"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := config.Load()

	stor, err := local.New(cfg.StorageDir)
	if err != nil {
		log.Error("init storage", "err", err)
		os.Exit(1)
	}

	repo, err := sqlite.New(cfg.DatabasePath)
	if err != nil {
		log.Error("init repository", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	svc := service.New(repo, stor, log)
	wh := webhooks.NewDispatcher(cfg.WebhookURLs, cfg.WebhookSecret, log)
	router := api.NewRouter(svc, stor, log, cfg, wh)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Minute, // large uploads
		IdleTimeout:  120 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Info("server starting", "addr", srv.Addr, "storage", cfg.StorageDir, "db", cfg.DatabasePath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	log.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("shutdown error", "err", err)
	}
}
