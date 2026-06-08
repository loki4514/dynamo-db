package storage

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
)

type badgerLogger struct {
	log zerolog.Logger
}

func (b badgerLogger) Errorf(format string, args ...interface{}) {
	b.log.Error().Msgf(format, args...)
}

func (b badgerLogger) Warningf(format string, args ...interface{}) {
	b.log.Warn().Msgf(format, args...)
}

func (b badgerLogger) Infof(format string, args ...interface{}) {
	b.log.Info().Msgf(format, args...)
}

func (b badgerLogger) Debugf(format string, args ...interface{}) {
	b.log.Debug().Msgf(format, args...)
}

func NewDB(log zerolog.Logger) (*badger.DB, error) {
	opts := badger.DefaultOptions("").
		WithInMemory(true).
		WithLogger(badgerLogger{log: log})

	return badger.Open(opts)
}
