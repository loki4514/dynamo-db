package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"dynamo-db/internal/wal"

	"github.com/gin-gonic/gin"
)

// forward proxies the current request to the owning peer's internal endpoint
// (which stores/reads directly, no re-routing — so this can never loop) and
// relays the peer's response back to the client.
func (s *Server) forward(c *gin.Context, owner, key string, body []byte) {
	addr, ok := s.peers.Addr(owner)
	if !ok {
		s.log.Error().Str("owner", owner).Str("key", key).Msg("no address for owner")
		c.JSON(http.StatusBadGateway, gin.H{"error": "no address for owner " + owner})
		return
	}

	url := fmt.Sprintf("http://%s/internal/keys/%s", addr, key)
	req, err := http.NewRequest(c.Request.Method, url, bytes.NewReader(body))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")

	s.log.Info().Str("key", key).Str("owner", owner).Str("method", c.Request.Method).Msg("forwarding")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.log.Error().Err(err).Str("owner", owner).Msg("forward failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "forward to " + owner + " failed: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	c.Data(resp.StatusCode, "application/json", respBody)
}

func (s *Server) getKey(c *gin.Context) {
	key := c.Param("key")
	owner, err := s.ring.KeyLookup(key)
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("lookup failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !s.peers.IsSelf(owner) {
		s.forward(c, owner, key, nil)
		return
	}

	value, err := s.node.Store.Get(key)
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("get failed")
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": value})
}

func (s *Server) getAllkey(c *gin.Context) {
	data, err := s.node.Store.IterateOverKeyAndValues()
	if err != nil {
		s.log.Error().Err(err).Msg("iterate failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

func (s *Server) putKey(c *gin.Context) {
	key := c.Param("key")

	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	owner, err := s.ring.KeyLookup(key)
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("lookup failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !s.peers.IsSelf(owner) {
		s.forward(c, owner, key, raw)
		return
	}

	var body struct {
		Value string `json:"value" binding:"required"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Value == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body, expected {\"value\": ...}"})
		return
	}
	if err := s.wal.Insert(wal.PUT, key, body.Value); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("WAL write failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "WAL write failed"})
		return
	}
	if err := s.node.Store.Put(key, body.Value); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("put failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": body.Value})
}

// fetchKey is the internal peer-to-peer read: serve directly from the local
// store, no re-routing (the caller already resolved this node as the owner).
func (s *Server) fetchKey(c *gin.Context) {
	key := c.Param("key")
	value, err := s.node.Store.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": value})
}

// receiveKey is the internal peer-to-peer handover endpoint. A peer that is
// draining its data sends a key it has decided THIS node now owns. Unlike
// putKey, it stores unconditionally — no rerouting — so it can never forward
// back and cause a loop. Still WAL'd, because it's a real durable write here.
func (s *Server) receiveKey(c *gin.Context) {
	key := c.Param("key")
	var body struct {
		Value string `json:"value" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := s.wal.Insert(wal.PUT, key, body.Value); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("WAL write failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "WAL write failed"})
		return
	}
	if err := s.node.Store.Put(key, body.Value); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("handover store failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.log.Info().Str("key", key).Msg("received handover key")
	c.JSON(http.StatusOK, gin.H{"key": key, "value": body.Value})
}

// removeKey is the internal peer-to-peer delete. A node that has handed a key
// off to its new owner tells the old holder to drop it. Deletes locally, no
// rerouting — mirror of receiveKey. Still WAL'd for crash recovery.
func (s *Server) removeKey(c *gin.Context) {
	key := c.Param("key")
	if err := s.wal.Insert(wal.DEL, key, ""); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("WAL write failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "WAL write failed"})
		return
	}
	if err := s.node.Store.Delete(key); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("internal delete failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	s.log.Info().Str("key", key).Msg("removed handed-off key")
	c.JSON(http.StatusOK, gin.H{"key": key})
}

func (s *Server) deleteKey(c *gin.Context) {
	key := c.Param("key")

	owner, err := s.ring.KeyLookup(key)
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("lookup failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !s.peers.IsSelf(owner) {
		s.forward(c, owner, key, nil)
		return
	}

	if err := s.wal.Insert(wal.DEL, key, ""); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("WAL write failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "WAL write failed"})
		return
	}
	if err := s.node.Store.Delete(key); err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("delete failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key})
}
