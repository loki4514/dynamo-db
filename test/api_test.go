package test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dynamo-db/internal/api"
	"dynamo-db/internal/config"
	"dynamo-db/internal/logger"
	"dynamo-db/internal/node"
	"dynamo-db/internal/storage"
	"dynamo-db/internal/wal"
)

// newTestServer builds a single-node server backed by an in-memory store and a
// temp-dir WAL. The peer list is just this node, so every key is owned locally
// and nothing forwards — good for exercising the local read/write path.
func newTestServer(t *testing.T) *api.Server {
	t.Helper()

	log := logger.New("test")

	db, err := storage.NewDB(log)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	store := storage.NewBadgerStore(db, log)

	w := wal.CreateWal("wal.txt", t.TempDir(), log)
	if err := w.CreateFile(); err != nil {
		t.Fatalf("create wal: %v", err)
	}

	n := node.NewNode("node-1", 1, store, w, log)
	peers, err := node.ParsePeers("node-1", []string{"node-1@localhost:8081"})
	if err != nil {
		t.Fatalf("parse peers: %v", err)
	}
	ring := node.CreateNodes(150, []node.NodeIdentification{{NodeName: "node-1"}}, log)

	cfg := &config.Config{}
	cfg.Server.Port = "8081"

	return api.NewServer(cfg, n, ring, peers, log, w)
}

// TestPutThenGet is the first smoke test: write a key, read it back, assert the
// value round-trips through the real HTTP handlers.
func TestPutThenGet(t *testing.T) {
	srv := newTestServer(t)
	h := srv.Handler()

	// PUT /keys/foo {"value":"bar"}
	putReq := httptest.NewRequest(http.MethodPut, "/keys/foo", strings.NewReader(`{"value":"bar"}`))
	putReq.Header.Set("Content-Type", "application/json")
	putRec := httptest.NewRecorder()
	h.ServeHTTP(putRec, putReq)

	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200; body=%s", putRec.Code, putRec.Body.String())
	}

	// GET /keys/foo
	getReq := httptest.NewRequest(http.MethodGet, "/keys/foo", nil)
	getRec := httptest.NewRecorder()
	h.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", getRec.Code, getRec.Body.String())
	}

	var resp struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode GET body: %v", err)
	}
	if resp.Value != "bar" {
		t.Errorf("value = %q, want %q", resp.Value, "bar")
	}
}
