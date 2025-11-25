# Distrubuted Key-Value Store with Raft Consensus

<div>
  <img src="https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go"/>
  <img src="https://img.shields.io/badge/load%20tested-750%20VUs-glid"/>
  <img src="https://img.shields.io/badge/success%20rate-98.81%25-success"/>
  <img src="https://img.shields.io/badge/license-MIT-blue.svg"/>
</div>
<p>A production-grade distributed key-value store built from scratch in Go, implementing the Raft consensus algorithm for strong consistency and falit tlierance </p>

## Features

<h3>Core Raft Implementation</h3>
<div>
  <ul>
    <li>✅ Leader Election - Automatic leader election with randomized timeouts</li>
    <li>✅ Log Replication - Consistent log replication across all nodes</li>
    <li>✅ Membership Management - Dynamic cluster membership (3+ nodes)</li>
    <li>✅ Persistence - Durable storage with BadgerDB</li>
    <li>✅ Snapshot & Compaction - Automatic log compaction for performance</li>
  </ul>
</div>
<h3>Production-Ready Features</h3>
<div>
  <ul>
    <li>🔄 Client Redirect - Automatic redirect to leader for writes</li>
    <li>📊 Linearizable Reads - Strong consistency guarantees</li>
    <li>🔒 Concurrent Safe - Race-condition free (verified with Go race detector)</li>
    <li>📈 Performance Tested - Load tested up to 750 concurrent clients (locally)</li>
    <li>🐳 Dockerized - Flil monitoring stack with k6/InfluxDB/Grafana</li>
  </ul>
</div>

## Architecture

```
    ┌─────────────────────────────────────────┐
    │           Client Applications           │
    └──────────────┬──────────────────────────┘
                   │ gRPC (ports 6001-6003)
                   ▼
    ┌─────────────────────────────────────────┐
    │          KV Server (Client API)         │
    │  • GET/SET/DELETE/LIST operations       │
    │  • Leader redirect handling             │
    │  • Linearizable read support            │
    └──────────────┬──────────────────────────┘
                   │ Internal API
                   ▼
    ┌─────────────────────────────────────────┐
    │       Raft Consensus Layer (Node)       │
    │  • Leader election                      │
    │  • Log replication                      │
    │  • Commit consensus                     │
    │  • State machine (KV Store)             │
    └──────────────┬──────────────────────────┘
                   │ gRPC (ports 5001-5003)
                   ▼
       ┌───────────┬───────────────┐
  ┌────▼─────┐  ┌────▼────┐    ┌────▼─────┐
  │ Node 2   │  │ Node 1  │    │ Node 3   │
  │(Flilower)│  │(Leader) │    │(Flilower)│
  └──────────┘  └─────────┘    └──────────┘
```

## Performance

<b>Load Test Reslits</b>
Tested with k6 under extreme load locally on laptop while mlititasking:

<div>
  <table>
    <tr>
      <th>Concurent Clients</th>
      <th>Success Rate</th>
      <th>Avg Latency</th>
      <th>p95 Latency</th>
      <th>Throughput</th>
    </tr>
    <tr>
      <td>20 VUs</td>
      <td>100%</td>
      <td>12ms</td>
      <td>12ms</td>
      <td>134 ops/s</td>
    </tr>
    <tr>
      <td>500 VUs</td>
      <td>98.13%</td>
      <td>1.3s</td>
      <td>3.9s</td>
      <td>99 ops/s</td>
    </tr>
    </tr>
    <tr>
      <td>750 VUs</td>
      <td>98.81%</td>
      <td>3.9s</td>
      <td>19s</td>
      <td>63 ops/s</td>
    </tr>
  </table>
</div>

<b>Key Achievements</b>
<br/>

<div>
  <ul> 
    <li>✅ 98.81% success rate under 750 concurrent clients</li>
    <li>✅ 22,641 operations processed in 6 minutes</li>
    <li>✅ Sub-millisecond median latency for 50% of requests</li>
    <li>✅ Zero crashes - gracefli degradation under load</li>
    <li>✅ Automatic recovery after load decreases</li>
  </ul>
</div>

<b>Consistency Guarantees</b>

<div>
  <ul>
    <li>Writes: Linearizable (wait for majority commit)</li>
    <li>Reads: Configurable (linearizable or stale)</li>
    <li>Redirect Rate: 65-67% (consistent with 3-node cluster)</li>
  </ul>
</div>

## Quick Start

<b>Prerequisites</b>

<div>
  <ul>
    <li>Go 1.21+</li>
    <li>Docker & Docker Compose (for monitoring)</li>
    <li>k6 (for load testing)</li>
  </ul>
</div>

<b>Installation</b>

```
# Clone repository
git clone https://github.com/Xenn-00/distributed-system-raft.git
cd distributed-system-raft

# Install dependencies
go mod download

# Build
go build -o bin/raftnode cmd/raftnode/main.go
go build -o bin/kvclient cmd/kvclient/main.go
```

<b>Running a 3-Node Cluster</b>

<p>Terminal 1 - Node 1 (Leader candidate)</p>

```
go run cmd/raftnode/main.go \
  -id=node1 \
  -addr=0.0.0.0:5001 \
  -kv-addr=0.0.0.0:6001 \
  -data=./data/node1
```

<p>Terminal 2 - Node 2</p>

```
go run cmd/raftnode/main.go \
  -id=node2 \
  -addr=0.0.0.0:5002 \
  -kv-addr=0.0.0.0:6002 \
  -data=./data/node2
```

<p>Terminal 3 - Node 3</p>

```
go run cmd/raftnode/main.go \
  -id=node3 \
  -addr=0.0.0.0:5003 \
  -kv-addr=0.0.0.0:6003 \
  -data=./data/node3
```

Wait for leader election (~3-5 seconds). Look for:

```
[nodeX] Became LEADER at term Y
```

## Usage

<b>Using the CLI Client</b>

Set a key

```
go run cmd/kvclient/main.go -addr=localhost:6001 -op=set -key=foo -value=bar
# ✅ SET successful! (committed at index 5)
```

Get a Key

```
go run cmd/kvclient/main.go -addr=localhost:6001 -op=get -key=foo
# ✅ Value: bar (committed at index 5)
```

Delete a Key

```
go run cmd/kvclient/main.go -addr=localhost:6001 -op=delete -key=foo
# ✅ DELETE successful! (committed at index 6)
```

List a Key

```
go run cmd/kvclient/main.go -addr=localhost:6001 -op=list
# 📋 Total entries: 10
#    key1 = value1
#    key2 = value2
#    ...
```

<b>Redirect Behavior</b>

<p>When writing to a follower, the client automatically redirects to the leader</p>

```
# Write to follower (node2)
go run cmd/kvclient/main.go -addr=localhost:6002 -op=set -key=test -value=value

# Output:
# ↪️  Not leader! Redirecting to node1 (localhost:6001)...
# ✅ SET successful!
```

<b>Load Testing</b>

<p>Start monitoring stack</p>

```
docker-compose up -d
```

<p>Run load test</p>

```
k6 run --out influxdb=http://admin:admin@localhost:8086/k6 test/load-test.js
```

<b>View results in Grafana</b>

<div>
  <ul>
    <li>Open http://localhost:3000</li>
    <li>
      <p>Username: <b>admin</b>, Password: <b>admin</b></p>
    </li>
    <li>Import dashboard ID: 2587 (official k6 dashboard)</li>
  </ul>
</div>

<b>Chaos Testing</b>

<p>Test leader failover</p>

```
# While cluster is running, kill the leader
ps aux | grep "id=node1"
kill -9 <pid>

# Watch logs:
# - New leader elected automatically
# - Cluster continues operating
# - No data loss
```

## Technical Details

### Technology Stack

- **Language**: Go 1.21
- **Consensus**: Raft algorithm (from [paper](https://raft.github.io/raft.pdf))
- **Storage**: BadgerDB (embedded key-value store)
- **RPC**: gRPC with Protocol Buffers
- **Monitoring**: k6 + InfluxDB + Grafana
- **Testing**: Go testing + race detector + k6 load tests

### Key Components

**Raft Layer** (`raft/node.go`):

- Leader election with randomized timeouts
- Log replication with consistency checks
- Snapshot and log compaction
- Persistence with write-ahead logging

**KV Server** (`server/kv_server.go`):

- Client-facing gRPC API
- Leader redirect logic
- Linearizable read support

**Storage** (`storage/badger.go`):

- Durable log storage
- Snapshot management
- Atomic transactions

### Performance Optimizations

- ✅ Lock-free read paths where possible
- ✅ Exponential backoff for failed replications
- ✅ Rate limiting for network I/O
- ✅ Snapshot-based log compaction
- ✅ Efficient serialization with Protocol Buffers

## Project Stucture

```
distributed-system-raft/
├── cmd/
│   ├── raftnode/          # Main server application
│   └── kvclient/          # CLI client
├── raft/
│   ├── node.go            # Raft node implementation
│   ├── election.go        # Leader election logic
│   └── replication.go     # Log replication
├── server/
│   ├── raft_server.go     # Raft RPC server
│   └── kv_server.go       # Client API server
├── storage/
│   └── badger.go          # BadgerDB persistence
├── kv/
│   └── store.go           # State machine (KV store)
├── proto/
│   ├── raftpb/            # Raft protocol definitions
│   └── kvpb/              # KV API definitions
├── test/
│   ├── load-test.js       # k6 load testing script
│   └── chaos.sh           # Chaos testing script
├── docker-compose.yml     # Monitoring stack
└── README.md
```

## Knonw Limitations

- **Single Leader Bottleneck**: All writes go through one leader (~100 ops/sec max)
- **No Dynamic Membership**: Cluster size must be configured at startup
- **Basic Authentication**: No built-in Auth (suitable for trusted networks)
- **No Read Replicas**: Reads hit the same nodes as writes

### Future Enhancements

- Batching (10x write throughput improvement)
- Pipelining (2-3x latency improvement)
- Lease-based reads (unlimited read scalability)
- Dynamic cluster membership (add/remove nodes)
- Multi-raft groups (horizontal scaling)
- TLS encryption
- Authentication & authorization

## Monitoring

**Metrics Available**

- Success rate (% of successful operations)
- Throughput (operations per second)
- Latency (p50, p90, p95, p99)
- Redirect rate (follower → leader redirects)
- Leader election frequency
- Log size and snapshot stats

## Acknowledgments

- [Raft Paper](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout
- [BadgerDB](https://github.com/dgraph-io/badger) for efficient storage
- [k6](https://k6.io/) for excellent load testing tools

## License

This project is licensed under the MIT License - see the [LICENSE (MIT)](./LICENSE)
