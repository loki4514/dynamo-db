package api

import (
	"context"
	"net/http"
	"time"

	"dynamo-db/internal/config"
	"dynamo-db/internal/node"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

type Server struct {
	http *http.Server
	node *node.Node
	log  zerolog.Logger
}

func NewServer(cfg *config.Config, n *node.Node, log zerolog.Logger) *Server {
	if cfg.Primary.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger(log))

	s := &Server{node: n, log: log}
	s.registerRoutes(r)

	s.http = &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      r,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.Server.IdleTimeout) * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	s.log.Info().Str("addr", s.http.Addr).Msg("server starting")
	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info().Msg("server shutting down")
	return s.http.Shutdown(ctx)
}

func requestLogger(log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		log.Info().
			Int("status", c.Writer.Status()).
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Msg("request")
	}
}
