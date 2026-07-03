package node

import (
	"fmt"
	"strings"
)

type Peers struct {
	self  string
	addrs map[string]string
}

// ParsePeers builds a Peers from a static config list of "name@host:port"
// entries (the list INCLUDES this node itself). self is this node's name,
// matched against the entries so it knows which one is "me".
//
// This is configuration, not discovery: the full membership is handed in at
// startup, not learned at runtime.
func ParsePeers(self string, entries []string) (*Peers, error) {
	addrs := make(map[string]string)
	for _, raw := range entries {
		// A single entry may itself be a comma-joined list: koanf's env
		// provider does NOT split "a,b,c" into a slice (only the .env file
		// parser does), so we split here to handle both sources.
		for _, entry := range strings.Split(raw, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			name, addr, ok := strings.Cut(entry, "@")
			if !ok {
				return nil, fmt.Errorf("bad peer entry %q: want name@host:port", entry)
			}
			addrs[name] = addr
		}
	}

	if _, ok := addrs[self]; !ok {
		return nil, fmt.Errorf("this node %q is not in its own peer list", self)
	}

	return &Peers{self: self, addrs: addrs}, nil
}

// Names returns every node name in the cluster — the membership list the ring
// is built from.
func (p *Peers) Names() []string {
	names := make([]string, 0, len(p.addrs))
	for name := range p.addrs {
		names = append(names, name)
	}
	return names
}

// NewPeers creates a Peers instance with the local node.
func NewPeers(self, addr string) *Peers {
	return &Peers{
		self: self,
		addrs: map[string]string{
			self: addr,
		},
	}
}

func (p *Peers) IsSelf(name string) bool {
	return p.self == name
}

// AddPeers merges the given peers into the existing peer list.
func (p *Peers) AddPeers(peers map[string]string) {
	if p.addrs == nil {
		p.addrs = make(map[string]string)
	}

	for name, addr := range peers {
		p.addrs[name] = addr
	}
}

func (p *Peers) Addr(name string) (string, bool) {
	addr, ok := p.addrs[name]
	return addr, ok
}
