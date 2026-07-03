package api

import "github.com/gin-gonic/gin"

func (s *Server) registerRoutes(r *gin.Engine) {
	// public client API
	keys := r.Group("/keys")
	{
		keys.GET("/:key", s.getKey)
		keys.PUT("/:key", s.putKey)
		keys.DELETE("/:key", s.deleteKey)
	}

	// dump every key on THIS node — used by a peer pulling data during a drain.
	// Kept off /keys to avoid colliding with the /keys/:key wildcard.
	r.GET("/get-all-keys", s.getAllkey)

	// internal peer-to-peer endpoints — node-to-node, not for clients.
	internal := r.Group("/internal")
	{
		internal.GET("/keys/:key", s.fetchKey)
		internal.PUT("/keys/:key", s.receiveKey)
		internal.DELETE("/keys/:key", s.removeKey)
	}
}
