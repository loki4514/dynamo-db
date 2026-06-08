package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// New returns a zerolog.Logger configured for the given environment.
// In "production" the output is JSON; otherwise it is human-readable console format.
func New(env string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339

	if env == "production" {
		return zerolog.New(os.Stdout).With().Timestamp().Logger()
	}

	console := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
	return zerolog.New(console).With().Timestamp().Logger()
}
