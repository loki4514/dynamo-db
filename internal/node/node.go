package node

import (
	"dynamo-db/internal/storage"

	"github.com/rs/zerolog"
)

type Node struct {
	Store storage.Storage
	log   zerolog.Logger
}

func NewNode(store storage.Storage, log zerolog.Logger) *Node {
	return &Node{Store: store, log: log}
}
