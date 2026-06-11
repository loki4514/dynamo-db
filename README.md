# Dynamo-DB

A distributed key-value store built in Go, inspired by DynamoDB. Uses BadgerDB as the storage engine with a Write-Ahead Log (WAL) for crash recovery.

## Stack

- **Go 1.25**
- **BadgerDB** — embedded key-value storage engine
- **Gin** — HTTP framework
- **Zerolog** — structured logging

## Project Structure

```
dynamo-db/
├── cmd/node/main.go          # entrypoint — startup, WAL recovery, server
├── internal/
│   ├── api/                  # HTTP server, routes, handlers
│   ├── config/               # env-based config loading
│   ├── logger/               # zerolog setup
│   ├── node/                 # node struct
│   ├── storage/              # Storage interface + BadgerDB implementation
│   └── wal/                  # Write-Ahead Log
├── Dockerfile
└── .env
```

## Running Locally

```bash
go run ./cmd/node
```

Server starts on `http://localhost:8080`.

## Running with Docker

```bash
docker build -t dynamo-node .
docker run -p 8080:8080 dynamo-node
```

## API

### Put a key
```bash
curl -X PUT http://localhost:8080/keys/foo \
  -H "Content-Type: application/json" \
  -d '{"value": "bar"}'
```

### Get a key
```bash
curl http://localhost:8080/keys/foo
```

### Delete a key
```bash
curl -X DELETE http://localhost:8080/keys/foo
```

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
