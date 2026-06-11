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
	"dynamo-db/internal/wal"

	"github.com/rs/zerolog"
)

func badgerStartup(walReader *wal.WALReader, store storage.Storage, log zerolog.Logger) error {
	for {
		entries, hasMore, err := walReader.NextBatch(100)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			log.Info().Str("op", string(entry.OperationType)).Str("key", entry.Key).Msg("replaying WAL entry")
			switch entry.OperationType {
			case wal.PUT:
				if err := store.Put(entry.Key, entry.Value); err != nil {
					return err
				}
			case wal.DEL:
				if err := store.Delete(entry.Key); err != nil {
					return err
				}
			}
		}
		if !hasMore {
			break
		}
	}
	return nil
}

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

	w := wal.CreateWal("wal.txt", "./data/", log)
	if err = w.CreateFile(); err != nil {
		log.Fatal().Err(err).Msg("failed to create WAL file")
	}

	store := storage.NewBadgerStore(db, log)

	// crash recovery — replay WAL into BadgerDB before accepting traffic
	walReader, err := w.NewReader()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open WAL for recovery")
	}
	if err := badgerStartup(walReader, store, log); err != nil {
		log.Fatal().Err(err).Msg("WAL recovery failed")
	}
	walReader.Close()

	n := node.NewNode(store, log)
	srv := api.NewServer(cfg, n, log, w)

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
