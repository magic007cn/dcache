# DCache - Distributed In-Memory Cache

A distributed in-memory cache system built with Go, using Raft consensus protocol for data consistency across multiple nodes.

## Features

- **Distributed Architecture**: Multi-node cluster with automatic leader election
- **Raft Consensus**: Uses HashiCorp's Raft implementation for data consistency
- **In-Memory Storage**: Fast BadgerDB-based storage with persistence
- **Range Scans**: Support for prefix-based key scanning
- **Automatic Recovery**: Nodes can rejoin cluster and recover data
- **Leadership Transfer**: Manual leadership transfer capability
- **REST API**: HTTP API for cache operations
- **Command Line Client**: CLI tool for testing and administration

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Node 1        │    │   Node 2        │    │   Node 3        │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │   API     │  │    │  │   API     │  │    │  │   API     │  │
│  │  Server   │  │    │  │  Server   │  │    │  │  Server   │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │   Raft    │◄─┼────┼─►│   Raft    │◄─┼────┼─►│   Raft    │  │
│  │ Manager   │  │    │  │ Manager   │  │    │  │ Manager   │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │  BadgerDB │  │    │  │  BadgerDB │  │    │  │  BadgerDB │  │
│  │  Storage  │  │    │  │  Storage  │  │    │  │  Storage  │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.21 or later
- Make (optional, for using Makefile)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd dcache
```

2. Install dependencies:
```bash
make deps
# or
go mod download
```

3. Build the project:
```bash
make build
# or
go build -o bin/dcache cmd/dcache/main.go
go build -o bin/dcache-client cmd/dcache-client/main.go
```

### Running a Single Node

1. Start the server:
```bash
./bin/dcache --config config.yaml
```

2. In another terminal, test with the client:
```bash
./bin/dcache-client set key1 value1
./bin/dcache-client get key1
./bin/dcache-client status
```

### Running a Multi-Node Cluster

1. Create multi-node configurations:
```bash
make create-configs
```

2. Start a 3-node cluster:
```bash
make run-cluster
```

3. Test the cluster:
```bash
./bin/dcache-client --server http://localhost:8080 set key1 value1
./bin/dcache-client --server http://localhost:8080 get key1
./bin/dcache-client --server http://localhost:8080 status
```

4. Stop the cluster:
```bash
make stop-cluster
```

## Configuration

The system uses a YAML configuration file. Key parameters:

```yaml
# Cluster configuration
cluster-id: "dcache-cluster-1"

# Data directory for storage
data-dir: "./data"

# Client API endpoints
listen-client-urls: "http://127.0.0.1:8080"
advertise-client-urls: "http://127.0.0.1:8080"

# Raft peer endpoints
listen-peer-urls: "http://127.0.0.1:8081"
initial-advertise-peer-urls: "http://127.0.0.1:8081"

# Initial cluster configuration
initial-cluster: "node1=http://127.0.0.1:8081,node2=http://127.0.0.1:8082"

# Log level
log-level: "debug"
```

## API Reference

### Cache Operations

#### GET /api/v1/get/{key}
Get a value by key.

**Response:**
```json
{
  "success": true,
  "data": {
    "key": "mykey",
    "value": "myvalue"
  }
}
```

#### POST /api/v1/set
Set a key-value pair.

**Request:**
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```

#### DELETE /api/v1/delete/{key}
Delete a key.

#### GET /api/v1/keys?prefix={prefix}&limit={limit}
Range scan keys with a prefix.

### Cluster Operations

#### GET /api/v1/cluster/status
Get cluster status.

#### GET /api/v1/cluster/leader
Get current leader.

#### POST /api/v1/cluster/transfer-leadership?target={node-id}
Transfer leadership to another node.

#### POST /api/v1/cluster/join?leader={leader-addr}
Join an existing cluster.

## Client Usage

### Basic Operations

```bash
# Set a key-value pair
./bin/dcache-client set key1 value1

# Get a value
./bin/dcache-client get key1

# Delete a key
./bin/dcache-client delete key1

# Range scan with prefix
./bin/dcache-client range-scan user:

# Get cluster status
./bin/dcache-client status

# Get current leader
./bin/dcache-client leader
```

### Advanced Usage

```bash
# Connect to a specific server
./bin/dcache-client --server http://192.168.1.100:8080 get key1

# Set timeout
./bin/dcache-client --timeout 30s set key1 value1
```

## Command Line Options

### Server Options

- `--config`: Configuration file path
- `--log-level`: Log level (debug, info, warn, error)

### Client Options

- `--server`: Server address (default: http://localhost:8080)
- `--timeout`: Request timeout (default: 10s)

## Development

### Project Structure

```
dcache/
├── cmd/
│   ├── dcache/          # Server main
│   └── dcache-client/   # Client main
├── internal/
│   ├── config/          # Configuration management
│   ├── raft/            # Raft consensus layer
│   ├── server/          # HTTP API server
│   └── storage/         # BadgerDB storage layer
├── config.yaml          # Default configuration
├── Makefile             # Build and run scripts
└── README.md           # This file
```

### Building

```bash
# Build everything
make build

# Build server only
go build -o bin/dcache cmd/dcache/main.go

# Build client only
go build -o bin/dcache-client cmd/dcache-client/main.go
```

### Testing

```bash
# Run basic tests
make test

# Run single node
make run

# Run multi-node cluster
make run-cluster
```

### Code Quality

```bash
# Format code
make fmt

# Lint code
make lint
```

## Features in Detail

### 1. Distributed Architecture
- Multiple nodes can form a cluster
- Automatic leader election using Raft
- Data replication across all nodes

### 2. Data Consistency
- All writes go through the leader
- Raft ensures consistency across nodes
- Automatic failover when leader goes down

### 3. Persistence
- BadgerDB provides persistent storage
- Automatic snapshots for fast recovery
- Log-based replication for durability

### 4. Range Scans
- Prefix-based key scanning
- Configurable result limits
- Efficient iteration over key ranges

### 5. Cluster Management
- Dynamic node addition/removal
- Manual leadership transfer
- Automatic rejoin after node restart

### 6. Monitoring
- REST API for cluster status
- Leader information
- Node health checks

## Troubleshooting

### Common Issues

1. **Node won't start**: Check if the data directory exists and is writable
2. **Can't join cluster**: Ensure the leader is running and accessible
3. **Write failures**: Verify you're connecting to the leader node
4. **Network issues**: Check firewall settings and network connectivity

### Logs

The server logs important events including:
- Leader election changes
- Node join/leave events
- Data replication status
- Error conditions

### Health Checks

Use the health endpoint to check if the server is running:
```bash
curl http://localhost:8080/health
```

## License

This project is licensed under the MIT License. 