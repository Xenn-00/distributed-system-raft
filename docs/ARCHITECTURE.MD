# Architecture Overview

## System Components

### 1. Raft Layer (Consensus)

- **Leader Election**: Handles node failures and leader election
- **Log Replication**: Ensures all nodes have consistent logs
- **Commit Protocol**: Guarantees majority agreement before commit

### 2. KV Server (Client API)

- **Request Routing**: Redirects writes to leader
- **Consistency**: Provides linearizable operations
- **Error Handling**: Graceful failure modes

### 3. Storage Layer

- **Persistence**: BadgerDB for durable storage
- **Snapshots**: Automatic log compaction
- **Recovery**: Fast startup from snapshots

## Data Flow

### Write Path

```
Client → KVServer → Check Leader
                  ↓ (if not leader)
              Redirect to Leader
                  ↓ (if leader)
              Propose to Raft
                  ↓
          Replicate to Followers
                  ↓
         Commit (majority agrees)
                  ↓
           Apply to State Machine
                  ↓
          Return Success to Client
```

### Read Path (Linearizable)

```
Client → KVServer → Check Leader
                  ↓ (if not leader)
              Redirect to Leader
                  ↓ (if leader)
           Wait for Latest Commit
                  ↓
          Read from State Machine
                  ↓
          Return Value to Client
```

## Failure Scenarios

### Leader Failure

1. Followers detect missing heartbeats
2. Election timeout triggers
3. New leader elected (majority vote)
4. Clients redirect to new leader
5. Operations resume

### Network Partition

1. Minority partition cannot elect leader
2. Majority partition continues operating
3. Minority rejects writes (no quorum)
4. On heal: minority catches up via log replication

### Node Crash & Recovery

1. Node restarts and loads from disk
2. Joins cluster as follower
3. Catches up via AppendEntries or snapshot
4. Participates in consensus once caught up
