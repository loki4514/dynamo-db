package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dynamo-db/internal/api"
	"dynamo-db/internal/config"
	"dynamo-db/internal/logger"
	"dynamo-db/internal/node"
	"dynamo-db/internal/storage"
)

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		os.Exit(1)
	}

	log := logger.New(cfg.Primary.Env)

	db, err := storage.NewDB(log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open badger")
	}
	defer db.Close()

	store := storage.NewBadgerStore(db, log)
	n := node.NewNode(store, log)
	srv := api.NewServer(cfg, n, log)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("shutdown error")
	}

	log.Info().Msg("server stopped")
}
