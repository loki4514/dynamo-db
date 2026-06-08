package api

import "github.com/gin-gonic/gin"

func (s *Server) registerRoutes(r *gin.Engine) {
	keys := r.Group("/keys")
	{
		keys.GET("/:key", s.getKey)
		keys.PUT("/:key", s.putKey)
		keys.DELETE("/:key", s.deleteKey)
	}
}
