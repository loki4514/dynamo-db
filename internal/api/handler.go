package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func (s *Server) getKey(c *gin.Context) {
	key := c.Param("key")
	value, err := s.node.Store.Get(key)
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("get failed")
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": value})
}

func (s *Server) putKey(c *gin.Context) {
	key := c.Param("key")
	var body struct {
		Value string `json:"value" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.node.Store.Put(key, body.Value); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("put failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": body.Value})
}

func (s *Server) deleteKey(c *gin.Context) {
	key := c.Param("key")
	if err := s.node.Store.Delete(key); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("delete failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key})
}
