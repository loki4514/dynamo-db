# Dynamo-DB

A distributed key-value store built in Go, inspired by DynamoDB. Keys are partitioned across
a cluster of shared-nothing nodes using a consistent-hash ring with virtual nodes. Each node is
its own process with its own BadgerDB and Write-Ahead Log (WAL); any node can receive any
request and forwards it to the key's owner. No master, no shared state.

## Why a ring instead of `hash(key) % N`

The obvious way to spread keys over N nodes is `hash(key) % N`. It works until N changes. Add
or drop one node and N shifts, so *almost every key* now maps somewhere new and has to be moved.
For 3 nodes going to 4, roughly 3 in 4 keys relocate — a stampede every time the cluster resizes.

A consistent-hash ring fixes this. Nodes and keys are hashed onto the same circle; a key belongs
to the first node clockwise. Remove a node and only *its* keys move — to the next node along.
Add a node and it takes a slice from *one* neighbor. Everyone else stays put.

```
hash(key) % N                          consistent-hash ring

add a node → N changes →               add a node → it slots into
~all keys remap                        one spot → only the keys in
                                       that arc move

[k1 k2 k3 k4 k5 k6]                          A
 └──┴──┴──┴──┴──┘                          /   \
   every slot shifts                     D      B   ← new node D only
                                          \    /       steals from its arc
                                            C
```

Virtual nodes take it further: each physical node sits at ~150 spots on the ring, not one, so
load spreads evenly and a failure's keys scatter across many neighbors instead of dumping on one.

## Stack

- **Go 1.25**
- **BadgerDB** — embedded key-value storage engine
- **Gin** — HTTP framework
- **Zerolog** — structured logging

## Project Structure

```
dynamo-db/
├── cmd/node/main.go          # entrypoint — startup, WAL recovery, ring, server
├── internal/
│   ├── api/                  # HTTP server, routes, handlers (incl. request forwarding)
│   ├── config/               # env-based config loading (.env optional)
│   ├── logger/               # zerolog setup
│   ├── node/                 # node struct, consistent-hash ring, peers, data movement
│   ├── storage/              # Storage interface + BadgerDB implementation
│   └── wal/                  # Write-Ahead Log
├── test/                     # httptest-driven API tests
├── Dockerfile
├── docker-compose.yml        # runs a 3-node cluster
└── .env
```

## Running a 3-node cluster (Docker Compose)

```bash
docker compose up --build
```

Starts `node-1`, `node-2`, `node-3` on ports 8081–8083. It's the same binary run three
times with different env — the "three" comes from Compose, not a loop in `main.go`. Storage
is in-memory, so data is not persisted across restarts (fine for exercising routing).

## Running a single node locally

```bash
go run ./cmd/node
```

Reads config from `.env` (port, name, peers). Note: with multiple local nodes they would share
one `./data/wal.txt` — use Docker Compose to run more than one node.

## API

Hit **any** node — if it doesn't own the key it forwards to the node that does and relays the
response. So the same key returns the same value regardless of which node you query.

### Put a key
```bash
curl -X PUT http://localhost:8081/keys/foo \
  -H "Content-Type: application/json" \
  -d '{"value": "bar"}'
```

### Get a key (from a different node — it forwards to the owner)
```bash
curl http://localhost:8083/keys/foo
```

### Delete a key
```bash
curl -X DELETE http://localhost:8081/keys/foo
```

### Dump the keys this node holds locally
```bash
curl http://localhost:8081/get-all-keys
```

## Testing

```bash
go test ./test/
```

Tests drive the real HTTP handlers via `httptest` (no port binding) against an in-memory
node.

## How it Works

### Write path
1. WAL entry written to disk first
2. Key written to BadgerDB

### Crash recovery
On startup, the WAL is replayed line by line into BadgerDB before the server accepts any traffic. This ensures no writes are lost after a crash.

### WAL format
Each entry is a single line:
```
PUT,key=foo,value=bar
DEL,key=foo,value=
```

## Consistent Hashing

Keys are routed to nodes using a consistent-hash ring (`internal/node/ring.go`), so that
adding or removing a node only relocates that node's share of keys — not the whole keyspace
(unlike naive `hash(key) % N` modulo routing).

### Hash space
The ring is a circle over a 64-bit space. Any string is placed by taking the first 8 bytes
of its MD5 digest as a `uint64`. A key is owned by the **first node position clockwise** of
the key's hash, wrapping past the top of the circle back to the smallest position.

### Virtual nodes
Each physical node is spread across many **virtual nodes** (`distributionNumber`, ~150 in
practice). Each vnode is placed by hashing `"name:i"` — the node's stable identity plus the
vnode index. More positions per node → more even load distribution and more even
redistribution when a node is lost. Only the node **name** is hashed (its identity); the node
number is metadata and is never hashed.

### Lookup
`Ring.KeyLookup(key)` hashes the key, then binary-searches the sorted vnode positions for the
first position `>= hash` (O(log n)). If the key hashes past the last position, it wraps to the
first. Returns the owning node's name.

### Config
Cluster membership is static (fixed at launch) via env keys. The peer list is `name@host:port`
entries including this node itself — the node finds itself by matching `PRIMARY__NAME`:
```
PRIMARY__NAME=node-1                 # this node's identity (hashed onto the ring)
PRIMARY__NUMBER=1                    # metadata only
PRIMARY__PEERS=node-1@localhost:8081,node-2@localhost:8082,node-3@localhost:8083
```
Identity (name, hashed onto the ring) is separate from address (host:port, used for
networking). The name never changes; the address can.

## Routing & Forwarding

Every node holds an **identical** copy of the ring (all nodes build it from the same static
peer list with the same hash, so they agree without any coordinator). On a request:

1. `KeyLookup(key)` → the owning node's name (local, no network).
2. If that's this node → serve from the local store.
3. Otherwise → forward the request to the owner's internal endpoint (`/internal/keys/:key`,
   which stores/reads directly without re-routing, so forwarding can never loop) and relay
   the response back to the client.

## Adding / Removing a Node

Membership changes move only the affected slice of keys (the whole point of the ring):

- **Add** — insert the new node's vnodes, then pull from its clockwise successor(s) only the
  keys that now hash to it (`KeyLookup == newNode`), handing each to the new node and deleting
  it from the source.
- **Remove** — drain the leaving node: for each of its keys, `KeyLookup` the new owner and
  forward it there. Removing the node's vnodes reroutes everything else automatically.

Both mutate the ring *first*, so `KeyLookup` reflects the new ownership during the move.
