package node

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/rs/zerolog"
)

type VirtualNode struct {
	Pos      uint64
	NodeName string
}

type Ring struct {
	nodes     map[string][]uint64
	positions []VirtualNode
	log       zerolog.Logger
}

type NodeIdentification struct {
	NodeName   string
	NodeNumber uint
}

// hashString maps any string onto the ring's hash space (first 8 bytes of MD5).
// Vnode placement hashes "name:i" — identity only, never the node number.
func hashString(s string, log zerolog.Logger) uint64 {
	hash := md5.Sum([]byte(s))
	hashValue := binary.BigEndian.Uint64(hash[:8])
	log.Info().
		Str("hash_input", s).
		Uint64("hash", hashValue).
		Msg("Generated ring hash")

	return hashValue
}

func NewRing(nodes []NodeIdentification, log zerolog.Logger) *Ring {
	n := make(map[string][]uint64)
	pos := make([]VirtualNode, 0)

	for _, node := range nodes {
		hashValue := hashString(node.NodeName, log)

		n[node.NodeName] = append(n[node.NodeName], hashValue)

		pos = append(pos, VirtualNode{
			Pos:      hashValue,
			NodeName: node.NodeName,
		})
	}

	sort.Slice(pos, func(i, j int) bool {
		return pos[i].Pos < pos[j].Pos
	})

	return &Ring{
		nodes:     n,
		positions: pos,
		log:       log,
	}
}

// CreateNodes builds a ring where each physical node is spread across
// distributionNumber virtual nodes. Each vnode is placed by hashing "name:i"
// (identity + vnode index only — never the node number).
func CreateNodes(distributionNumber int, nodes []NodeIdentification, log zerolog.Logger) *Ring {
	n := make(map[string][]uint64)
	pos := make([]VirtualNode, 0)

	for i := 0; i < distributionNumber; i++ {
		for _, node := range nodes {
			vnodeID := fmt.Sprintf("%s:%d", node.NodeName, i)
			hashValue := hashString(vnodeID, log)

			n[node.NodeName] = append(n[node.NodeName], hashValue)

			pos = append(pos, VirtualNode{
				Pos:      hashValue,
				NodeName: node.NodeName,
			})
		}
	}

	sort.Slice(pos, func(i, j int) bool {
		return pos[i].Pos < pos[j].Pos
	})

	return &Ring{
		nodes:     n,
		positions: pos,
		log:       log,
	}
}

func (ring *Ring) KeyLookup(key string) (string, error) {
	if len(ring.positions) == 0 {
		return "", fmt.Errorf("ring is empty: no nodes to own key %q", key)
	}

	hashValue := hashString(key, ring.log)

	// successor search: smallest index whose position is >= hashValue
	low := 0
	high := len(ring.positions) - 1
	for low <= high {
		mid := low + (high-low)/2
		if ring.positions[mid].Pos < hashValue {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	// fell off the end → wrap around the circle to the first position
	if low == len(ring.positions) {
		low = 0
	}

	return ring.positions[low].NodeName, nil
}

func (ring *Ring) RemoveNodes(nodeName string) error {
	if len(ring.positions) == 0 {
		return fmt.Errorf("ring is empty")
	}

	if _, ok := ring.nodes[nodeName]; !ok {
		return fmt.Errorf("node name doesn't exist in the system: %s", nodeName)
	}

	// Drop every vnode owned by this node in a single pass. Filtering keeps
	// the slice sorted, so KeyLookup's binary search still works; the removed
	// node's keys reroute to their clockwise successors automatically on the
	// next lookup.
	kept := ring.positions[:0]
	for _, vn := range ring.positions {
		if vn.NodeName != nodeName {
			kept = append(kept, vn)
		}
	}
	ring.positions = kept

	delete(ring.nodes, nodeName)

	return nil
}

func (ring *Ring) AddNodes(distributionNumber int, nodeName string) error {
	if _, ok := ring.nodes[nodeName]; ok {
		return fmt.Errorf("node already exists in the system: %s", nodeName)
	}

	var n []uint64
	pos := make([]VirtualNode, 0)

	for i := 0; i < distributionNumber; i++ {
		vnodeID := fmt.Sprintf("%s:%d", nodeName, i)
		hashValue := hashString(vnodeID, ring.log)

		n = append(n, hashValue)

		pos = append(pos, VirtualNode{
			Pos:      hashValue,
			NodeName: nodeName,
		})
	}

	// merge the new vnodes with the existing ring, then re-sort so
	// KeyLookup's binary search still holds.
	pos = append(pos, ring.positions...)

	sort.Slice(pos, func(i, j int) bool {
		return pos[i].Pos < pos[j].Pos
	})

	ring.nodes[nodeName] = n
	ring.positions = pos

	return nil
}

func (ring *Ring) GetClockwiseNodes(nodeName string) ([]string, error) {
	if len(ring.positions) == 0 {
		return nil, nil
	}

	start := -1
	for i, pos := range ring.positions {
		if pos.NodeName == nodeName {
			start = i
			break
		}
	}

	if start == -1 {
		return nil, fmt.Errorf("node %q not found", nodeName)
	}

	seen := make(map[string]bool)
	nodes := make([]string, 0)

	// Traverse the ring once.
	for step := 1; step < len(ring.positions); step++ {
		idx := (start + step) % len(ring.positions)
		name := ring.positions[idx].NodeName

		if name == nodeName || seen[name] {
			continue
		}

		seen[name] = true
		nodes = append(nodes, name)
	}

	return nodes, nil
}
