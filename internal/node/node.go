package node

import (
	"bytes"
	"dynamo-db/internal/storage"
	"dynamo-db/internal/wal"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog"
)

type Node struct {
	NodeName   string // identity — hashed onto the ring; must be stable + unique
	NodeNumber int    // metadata only — never hashed (human/operational convenience)
	Store      storage.Storage
	wal        *wal.WAL // per-node durability; WAL-then-Store on every write
	log        zerolog.Logger
	isActive   bool
}

func NewNode(name string, number int, store storage.Storage, w *wal.WAL, log zerolog.Logger) *Node {
	return &Node{NodeName: name, NodeNumber: number, Store: store, wal: w, log: log, isActive: true}
}

func (node *Node) RemoveNodesDataMovement(nodeName string, peers *Peers, ring *Ring) error {
	endpoint, exists := peers.Addr(nodeName)
	if !exists {
		node.log.Error().Str("node", nodeName).Msg("unknown node")
		return fmt.Errorf("invalid name or node doesn't exist: %s", nodeName)
	}
	// endpoint is already host:port — just prefix scheme and the path.
	url := fmt.Sprintf("http://%s/get-all-keys", endpoint)
	resp, err := http.Get(url)
	if err != nil {
		node.log.Error().Err(err).Str("node", nodeName).Msg("fetch keys failed")
		return fmt.Errorf("fetch keys from %s: %w", nodeName, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		node.log.Error().Err(err).Str("node", nodeName).Msg("read response failed")
		return fmt.Errorf("read keys from %s: %w", nodeName, err)
	}

	// the /get-all-keys handler returns {"data": {key: value, ...}}
	var payload struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		node.log.Error().Err(err).Str("node", nodeName).Msg("unmarshal keys failed")
		return fmt.Errorf("unmarshal keys from %s: %w", nodeName, err)
	}

	// Remove the leaving node from the ring FIRST, so KeyLookup below returns
	// a surviving successor instead of the node we're draining.
	if err := ring.RemoveNodes(nodeName); err != nil {
		node.log.Error().Err(err).Str("node", nodeName).Msg("ring removal failed")
		return fmt.Errorf("remove %s from ring: %w", nodeName, err)
	}

	// Redistribute each key to its new owner. Log-and-continue on per-key
	// failures rather than aborting — a partial drain is recoverable, an
	// aborted-halfway one is not obviously better.
	var failed int
	for key, value := range payload.Data {
		owner, err := ring.KeyLookup(key)
		if err != nil {
			node.log.Error().Err(err).Str("key", key).Msg("lookup failed")
			failed++
			continue
		}

		if peers.IsSelf(owner) {
			if err := node.wal.Insert(wal.PUT, key, value); err != nil {
				node.log.Error().Err(err).Str("key", key).Msg("local WAL write failed")
				failed++
				continue
			}
			if err := node.Store.Put(key, value); err != nil {
				node.log.Error().Err(err).Str("key", key).Msg("local store failed")
				failed++
			}
			continue
		}

		if err := node.handoverKey(peers, owner, key, value); err != nil {
			node.log.Error().Err(err).Str("key", key).Str("owner", owner).Msg("handover failed")
			failed++
		}
	}

	node.log.Info().
		Str("drained", nodeName).
		Int("keys", len(payload.Data)).
		Int("failed", failed).
		Msg("data movement complete")

	if failed > 0 {
		return fmt.Errorf("data movement finished with %d failed keys", failed)
	}
	return nil
}

func (node *Node) AddNodesDataMovement(nodeName string, peers *Peers, ring *Ring, distributionNumber int) error {
	// Add the new node to the ring FIRST, so KeyLookup below returns the new
	// node for the keys that now fall into its arc.
	if err := ring.AddNodes(distributionNumber, nodeName); err != nil {
		node.log.Error().Err(err).Str("node", nodeName).Msg("ring addition failed")
		return fmt.Errorf("add %s to ring: %w", nodeName, err)
	}

	// Only the new node's clockwise successors can lose keys to it — scan just
	// those, not the whole cluster.
	successors, err := ring.GetClockwiseNodes(nodeName)
	if err != nil {
		node.log.Error().Err(err).Str("node", nodeName).Msg("retrieving successor nodes failed")
		return fmt.Errorf("retrieve successors of %q: %w", nodeName, err)
	}

	var failed int
	for _, successor := range successors {
		endpoint, exists := peers.Addr(successor)
		if !exists {
			node.log.Error().Str("node", successor).Msg("unknown node")
			return fmt.Errorf("invalid name or node doesn't exist: %s", successor)
		}

		url := fmt.Sprintf("http://%s/get-all-keys", endpoint)
		resp, err := http.Get(url)
		if err != nil {
			node.log.Error().Err(err).Str("node", successor).Msg("fetch keys failed")
			return fmt.Errorf("fetch keys from %s: %w", successor, err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			node.log.Error().Err(err).Str("node", successor).Msg("read response failed")
			return fmt.Errorf("read keys from %s: %w", successor, err)
		}

		// the /get-all-keys handler returns {"data": {key: value, ...}}
		var payload struct {
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			node.log.Error().Err(err).Str("node", successor).Msg("unmarshal keys failed")
			return fmt.Errorf("unmarshal keys from %s: %w", successor, err)
		}

		// Move only the keys that now belong to the NEW node — the rest stay
		// on the successor. KeyLookup is the boundary check.
		for key, value := range payload.Data {
			owner, err := ring.KeyLookup(key)
			if err != nil {
				node.log.Error().Err(err).Str("key", key).Msg("lookup failed")
				failed++
				continue
			}
			if owner != nodeName {
				continue // still the successor's key
			}

			// hand the key to the new node...
			if err := node.handoverKey(peers, nodeName, key, value); err != nil {
				node.log.Error().Err(err).Str("key", key).Str("owner", nodeName).Msg("handover failed")
				failed++
				continue
			}
			// ...then drop it from the successor (only after a successful handover).
			if err := node.deleteRemoteKey(endpoint, key); err != nil {
				node.log.Error().Err(err).Str("key", key).Str("node", successor).Msg("source delete failed")
				failed++
			}
		}
	}

	node.log.Info().
		Str("added", nodeName).
		Int("successors", len(successors)).
		Int("failed", failed).
		Msg("add data movement complete")

	if failed > 0 {
		return fmt.Errorf("add data movement finished with %d failed keys", failed)
	}
	return nil
}

// deleteRemoteKey tells a source node (at addr host:port) to drop a key it no
// longer owns, via the internal delete-directly endpoint (no rerouting).
func (node *Node) deleteRemoteKey(addr, key string) error {
	url := fmt.Sprintf("http://%s/internal/keys/%s", addr, key)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("build delete request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send delete to %s: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("source %s returned status %d on delete", addr, resp.StatusCode)
	}
	return nil
}

// handoverKey sends one key/value to its new owner's internal store-directly
// endpoint (no rerouting on the receiving side).
func (node *Node) handoverKey(peers *Peers, owner, key, value string) error {
	addr, ok := peers.Addr(owner)
	if !ok {
		return fmt.Errorf("no address for owner %q", owner)
	}

	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/keys/%s", addr, key)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send to %s: %w", owner, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("owner %s returned status %d", owner, resp.StatusCode)
	}
	return nil
}


