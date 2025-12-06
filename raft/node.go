package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/kv"
	"github.com/Xenn-00/distributed-kv-store/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewNode(id string, peers map[string]string, dataDir string) (*Node, error) {
	// Open Storage
	stor, err := storage.NewBadgerStroage(filepath.Join(dataDir, id))
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %v", err)
	}

	node := &Node{
		id:                  id,
		state:               Follower,
		peers:               peers,
		currentTerm:         0,
		votedFor:            "",
		log:                 make([]*pb.LogEntry, 0),
		commitIndex:         0,
		lastApplied:         0,
		leaderID:            "",
		nextIndex:           make(map[string]uint64),
		matchIndex:          make(map[string]uint64),
		shutdownCh:          make(chan struct{}),
		clients:             make(map[string]pb.RaftClient),
		kvStore:             kv.NewKVStore(),
		storage:             stor,
		lastSnapshotTime:    time.Now(),
		lastSnapshotIndex:   0,
		proposalQueue:       make(chan *proposalRequest, 200), // Max 200 queued
		ProposalSem:         make(chan struct{}, 500),         // Max 500 in-flight
		proposalStop:        make(chan struct{}),
		replicationQueue:    make(chan string, 128), // Buffer 128 tasks
		replicationStop:     make(chan struct{}),
		replicationFailures: make(map[string]int),
		replicators:         make(map[string]*PeerReplicator),
		lastFailureTime:     make(map[string]time.Time),
	}
	// Restore from disk
	if err := node.restoreFromStorage(); err != nil {
		return nil, fmt.Errorf("failed to restore from storage: %v", err)
	}
	return node, nil
}

// restoreFromStorage loads persistent state from disk
func (n *Node) restoreFromStorage() error {
	// Load term
	term, err := n.storage.LoadTerm()
	if err != nil {
		return err
	}
	n.currentTerm = term

	// Load vote
	votedFor, err := n.storage.LoadVote()
	if err != nil {
		return err
	}
	n.votedFor = votedFor

	// Load log
	logs, err := n.storage.GetAllLogs()
	if err != nil {
		return err
	}
	n.log = logs

	// Load snapshot if exist
	if n.storage.HasSnapshot() {
		lastIncludedIndex, lastIncludedTerm, data, err := n.storage.LoadSnapshot()
		if err != nil {
			return err
		}

		log.Printf("[%s] Loaded snapshot: lastIncludedIndex=%d, lastIncludedterm=%d", n.id, lastIncludedIndex, lastIncludedTerm)

		// Restore KV state from snapshot
		var kvState map[string]string
		if err := json.Unmarshal(data, &kvState); err != nil {
			return err
		}

		for k, v := range kvState {
			n.kvStore.Set(k, v)
		}

		n.lastApplied = lastIncludedIndex
		if len(n.log) > 0 {
			// Assume all restored logs were committed
			n.commitIndex = max(lastIncludedIndex, n.log[len(n.log)-1].Index)
		} else {
			n.commitIndex = lastIncludedIndex
		}

		log.Printf("[%s] Restored %d keys from snapshot", n.id, len(kvState))
	}

	log.Printf("[%s] Restored from storage: term=%d, votedFor=%s, log entries=%d", n.id, n.currentTerm, n.votedFor, len(n.log))

	// Replay log entries after snapshot
	if len(n.log) > 0 {
		// Optimized: only log every 10th entry during replay
		logCount := 0

		for _, entry := range n.log {
			// Validation: entry index should be after snapshot
			if n.storage.HasSnapshot() {
				snapIndex, _, _, _ := n.storage.LoadSnapshot()
				if entry.Index <= snapIndex {
					log.Printf("[%s] WARN: Entry %d is before/at snapshot %d, skipping", n.id, entry.Index, snapIndex)
					continue
				}
			}

			// Only apply entries after lastApplied
			if entry.Index > n.lastApplied {
				if err := n.kvStore.Apply(entry.Command); err != nil {
					log.Printf("[%s] Failed to apply entry %d: %v", n.id, entry.Index, err)
				}
				n.lastApplied = entry.Index
				logCount++

				// Sample logging
				if shouldLog(entry.Index, 10) {
					log.Printf("[%s] Replayed entry index=%d", n.id, entry.Index)
				}
			}
		}
	}
	log.Printf("[%s] Replay complete: lastApplied=%d", n.id, n.lastApplied)
	return nil
}

// Lazy get client with retry
func (n *Node) getClient(peerID string) (pb.RaftClient, error) {
	// Check if client already exists
	n.clientsMu.RLock()
	client, ok := n.clients[peerID]
	n.clientsMu.RUnlock()
	if ok {
		return client, nil
	}

	// Create new gRPC client (write-lock)
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()

	// Double-check after acquiring write lock
	if client, ok := n.clients[peerID]; ok {
		return client, nil
	}

	// Get peer address
	addr, ok := n.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID: %s", peerID)
	}

	// Create new client
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s at %s: %v", peerID, addr, err)
	}

	client = pb.NewRaftClient(conn)
	n.clients[peerID] = client
	log.Printf("[%s] Connected to peer %s at %s", n.id, peerID, addr)
	return client, nil
}

func (n *Node) Start() {
	initialDelay := time.Duration(rand.Int63n(2000)) * time.Millisecond
	log.Printf("[%s] Starting node as %s (initial delay: %v)", n.id, n.state, initialDelay)
	time.Sleep(initialDelay)
	n.mu.Lock()
	// Don't touch term! already loaded from storage
	// Just reset state
	n.state = Follower
	n.leaderID = ""

	// Clear votedFor if it was for ourselves (stale self-vote)
	if n.votedFor == n.id {
		n.votedFor = ""
		n.storage.SaveVote("")
	}
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.processProposalWithBatching()

	// go n.processProposals()

	// Start replication workers
	n.startReplicationWorkers()
	// Start periodic snapshot check
	go n.periodicSnapshotCheck()
	go n.reportFailures()
	go n.run()
}

func (n *Node) reportFailures() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()

			// Report peers with failures
			for peerID, failures := range n.replicationFailures {
				if failures > 0 {
					lastFail := n.lastFailureTime[peerID]
					log.Printf("[%s] Peer %s: %d consecutive failures (last: %v ago)", n.id, peerID, failures, time.Since(lastFail))
				}
			}
			n.mu.Unlock()
		case <-n.shutdownCh:
			return
		}
	}
}

func (n *Node) run() {
	for {
		select {
		case <-n.shutdownCh:
			return
		case <-n.electionTimer.C:
			n.startElection()
		}
	}
}

func (n *Node) Shutdown() {

	// Stop proposal processor first
	close(n.proposalStop)

	// Drain pending proposals
	close(n.proposalQueue)
	for req := range n.proposalQueue {
		req.respCh <- &proposalResponse{
			err: fmt.Errorf("node shutting down"),
		}
	}

	// Close all pending waiters
	n.commitWaiters.Range(func(key, value any) bool {
		if entry, ok := value.(*waitersEntry); ok {
			entry.mu.Lock()
			waiters := make([]chan struct{}, len(entry.waiters))
			copy(waiters, entry.waiters)
			entry.mu.Unlock()
			for _, ch := range waiters {
				close(ch)
			}
		}
		return true
	})
	n.applyWaiters.Range(func(key, value any) bool {
		if entry, ok := value.(*waitersEntry); ok {
			entry.mu.Lock()
			waiters := make([]chan struct{}, len(entry.waiters))
			copy(waiters, entry.waiters)
			entry.mu.Unlock()
			for _, ch := range waiters {
				close(ch)
			}
		}
		return true
	})

	// Stop worker pool
	close(n.replicationStop)

	close(n.shutdownCh)
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	if n.storage != nil {
		n.storage.Close()
	}
}
