package raft

import (
	"context"
	"log"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

func (n *Node) startReplicationWorkers() {
	// Start 25 workers (12 per peer for 2 peers)
	// This limits concurrent replication goroutines
	numWorkers := 15

	log.Printf("[%s] Starting %d replication workers", n.id, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go n.replicationWorkers(i)
	}
}

// worker goroutine that processes replication tasks
func (n *Node) replicationWorkers(workerID int) {
	for {
		select {
		case peerID := <-n.replicationQueue:
			// process replication task
			n.replicateToPeer(peerID)
		case <-n.replicationStop:
			log.Printf("[%s] Replication worker %d stopping", n.id, workerID)
			return
		case <-n.shutdownCh:
			return
		}
	}
}

// replicateToAll sends AppendEntries to all followers
func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	for peerID := range n.peers {
		if peerID == n.id {
			continue
		}
		// Non-blocking enqueue
		select {
		case n.replicationQueue <- peerID:
			// Task enqueued successfully
		default:
			// Queue full, skip (will retry on next heartbeat)
			// This prevents unbounded goroutine growth
		}
		// go n.replicateToPeer(peerID)
	}
	n.mu.Unlock()
}

// replicateToPeer sends AppendEntries to a specific peer
func (n *Node) replicateToPeer(peerID string) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	// Rate limiting: Skip if too many recent failures
	failures := n.replicationFailures[peerID]
	lastFail := n.lastFailureTime[peerID]

	if failures > 0 {
		// Exponential backoff: 100ms, 200ms, 400ms, 800ms, max 5s
		backoff := min(time.Duration(100*(1<<uint(failures-1)))*time.Millisecond, 5*time.Second)

		if time.Since(lastFail) < backoff {
			// Too soon to retry, skip
			n.mu.Unlock()
			return
		}
	}

	// Get next index for this peer
	nextIdx := n.nextIndex[peerID]
	if nextIdx == 0 {
		nextIdx = 1
	}

	// Check if we need to send snapshot
	var firstLogIndex uint64 = 1
	if len(n.log) > 0 {
		firstLogIndex = n.log[0].Index
	} else if n.storage.HasSnapshot() {
		snapIndex, _, _, _ := n.storage.LoadSnapshot()
		firstLogIndex = snapIndex + 1
	}

	// If nextIndex is behind our first log entry, send snapshot
	if nextIdx < firstLogIndex {
		log.Printf("[%s] Peer %s is too far behind (nextIndex=%d, firstLogIndex=%d)", n.id, peerID, nextIdx, firstLogIndex)
		n.mu.Unlock()
		n.sendSnapshot(peerID)
		return
	}

	// Prepare AppendEntries request
	req := n.prepareAppendEntriesRequest(nextIdx)
	term := n.currentTerm
	leaderCommit := n.commitIndex
	req.LeaderCommit = leaderCommit
	n.mu.Unlock()

	// Send RPC
	success, higherTerm := n.sendAppendEntries(peerID, req, term)

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if still leader
	if n.state != Leader || n.currentTerm != term {
		return
	}

	// Handle higher term
	if higherTerm > 0 {
		n.handleHigherTerm(higherTerm)
		return
	}

	// Handle response
	if success {
		n.handleSuccessfulReplication(peerID, req.Entries)
	} else {
		n.handleFailedReplication(peerID)
	}
}

// prepareAppendEntriesRequest contructs an AppendEntries request
func (n *Node) prepareAppendEntriesRequest(nextIdx uint64) *pb.AppendEntriesRequest {
	// Get entries to send
	var entries []*pb.LogEntry
	lastLogIndex := n.getLastLogIndex()

	if nextIdx <= lastLogIndex {
		for _, entry := range n.log {
			if entry.Index >= nextIdx {
				entries = append(entries, entry)
			}
		}
	}

	// Get previous log entry info
	var prevLogIndex, prevLogTerm uint64
	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		prevLogTerm = n.getLogTermAtIndex(prevLogIndex)
	}

	return &pb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

// sendAppendEntries sends AppendEntries RPC and returns (success, higherTerm)
func (n *Node) sendAppendEntries(peerID string, req *pb.AppendEntriesRequest, term uint64) (bool, uint64) {
	client, err := n.getClient(peerID)
	if err != nil {
		n.recordReplicationFailure(peerID, len(req.Entries) > 10)
		return false, 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		n.recordReplicationFailure(peerID, len(req.Entries) > 10)
		return false, 0
	}

	// Check for higher term
	if resp.Term > term {
		return false, resp.Term
	}

	return resp.Success, 0
}

// handleSuccessfulReplication updates matchIndex and nextIndex after successful replication
func (n *Node) handleSuccessfulReplication(peerID string, entries []*pb.LogEntry) {
	n.replicationFailures[peerID] = 0
	delete(n.lastFailureTime, peerID)

	if len(entries) > 0 {
		lastIdx := entries[len(entries)-1].Index
		n.matchIndex[peerID] = lastIdx
		n.nextIndex[peerID] = lastIdx + 1
		if shouldLog(lastIdx, 10) {
			log.Printf("[%s] Peer %s replicated up to index %d", n.id, peerID, lastIdx)
		}
		n.updateCommitIndexWithBatching()
	}
}

// handleFailedReplication handles AppendEntries rejection
func (n *Node) handleFailedReplication(peerID string) {
	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
		if shouldLog(n.nextIndex[peerID], 5) {
			log.Printf("[%s] Peer %s rejected, decrementing nextIndex to %d",
				n.id, peerID, n.nextIndex[peerID])
		}
	}
}

// handleHigherTerm steps down when receiving a higher term
func (n *Node) handleHigherTerm(higherTerm uint64) {
	log.Printf("[%s] Stepping down: received higher term %d", n.id, higherTerm)
	n.currentTerm = higherTerm
	n.votedFor = ""
	n.leaderID = ""
	n.storage.SaveTerm(n.currentTerm)
	n.storage.SaveVote(n.votedFor)
	n.becomeFollower(higherTerm)
}

// recordReplicationFailure tracks consecutive failures for rate limiting
func (n *Node) recordReplicationFailure(peerID string, shouldLog bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.replicationFailures[peerID]++
	n.lastFailureTime[peerID] = time.Now()

	if shouldLog && n.replicationFailures[peerID] <= 3 {
		log.Printf("[%s] Replication to %s failed (failure %d)", n.id, peerID, n.replicationFailures[peerID])
	}
}

func (n *Node) sendHeartbeats() {
	for {
		select {
		case <-n.heartbeatTimer.C:
			n.replicateToAll()
		case <-n.shutdownCh:
			return
		}
	}
}
