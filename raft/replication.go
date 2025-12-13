package raft

import (
	"context"
	"log"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

// triggerReplication an helper to trigger replication (non-blocking): now no-op
func (n *Node) triggerReplication() {
	// select {
	// case n.replicationSignal <- struct{}{}:
	// 	// signal sent
	// default:
	// 	// already signaled
	// }
}

// replicateToAll sends AppendEntries to all followers
func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	// Check if using pipelined replication
	n.replicatorsMu.RLock()
	hasPipeline := len(n.replicators) > 0
	n.replicatorsMu.RUnlock()

	// Sample logging: every 10th heartbeat
	n.heartbeatCount++
	sLog := shouldLog(n.heartbeatCount, 10)
	n.mu.Unlock()

	if !hasPipeline {
		log.Printf("[%s] CRITICAL: No pipeline active! Starting...", n.id)
		go n.StartPipelinedReplication()
		return
	}

	// Check pipeline health
	n.lastHeartbeatAckMu.Lock()
	stalePeers := []string{}
	for peerID := range n.peers {
		if peerID == n.id {
			continue
		}

		lastAck, ok := n.lastHeartbeatAck[peerID]
		if !ok || time.Since(lastAck) > 5*HeartbeatInterval {
			stalePeers = append(stalePeers, peerID)
		}
	}
	n.lastHeartbeatAckMu.Unlock()
	if len(stalePeers) > 0 && sLog {
		log.Printf("[%s] WARNING: Stale peers: %v", n.id, stalePeers)

		// Check failure counts
		n.mu.Lock()
		for _, peerID := range stalePeers {
			failures := n.replicationFailures[peerID]
			if failures > 0 {
				log.Printf("[%s] - %s: %d consecutive failures", n.id, peerID, failures)
			}
		}
		n.mu.Unlock()
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

// sendAppendEntriesRPC sends AppendEntries RPC and returns (success, higherTerm)
func (n *Node) sendAppendEntriesRPC(peerID string, req *pb.AppendEntriesRequest, term uint64) (bool, uint64, error) {
	client, err := n.getClient(peerID)
	if err != nil {
		log.Printf("[%s] Failed to get client for %s: %v", n.id, peerID, err)
		return false, 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := client.AppendEntries(ctx, req)
	latency := time.Since(start)
	if err != nil {
		log.Printf("[%s] AppendEntries to %s FAILED (latency: %v): %v", n.id, peerID, latency, err)
		return false, 0, err
	}

	// Log slow RPCs
	if latency > 500*time.Millisecond {
		log.Printf("[%s] SLOW AppendEntries to %s: %v", n.id, peerID, latency)
	}

	// Check for higher term
	if resp.Term > term {
		return false, resp.Term, nil
	}

	return resp.Success, 0, nil
}

// updatePeerIndices updates nextIndex and matchIndex after replication attempt.
// Must be called with n.mu held!
func (n *Node) updatePeerIndices(peerID string, success bool, matchIndex uint64) {
	if success && matchIndex > 0 {
		// Success: advance indices
		oldMatch := n.matchIndex[peerID]
		n.matchIndex[peerID] = matchIndex
		n.nextIndex[peerID] = matchIndex + 1

		// Sample logging
		if shouldLog(matchIndex, 10) {
			log.Printf("[%s] Peer %s: matchIndex %d->%d, nextIndex=%d", n.id, peerID, oldMatch, matchIndex, n.nextIndex[peerID])
		}

		// Try to advance commit index
		n.updateCommitIndexWithBatching()
	} else if !success {
		// Consistency check failed: backtrack nextIndex
		if n.nextIndex[peerID] > 1 {
			oldNext := n.nextIndex[peerID]
			n.nextIndex[peerID]--

			if shouldLog(n.nextIndex[peerID], 5) {
				log.Printf("[%s] Peer %s: consistency failed, nextIndex %d->%d", n.id, peerID, oldNext, n.nextIndex[peerID])
			}
		}
	}
}

// recordReplicationOutcome tracks replication success/failure for monitoring.
// Updates failure counters and hearbeat ack timestamps.
func (n *Node) recordReplicationOutcome(peerID string, success bool, err error) {
	if err != nil || !success {
		// Failure: increment counter
		n.replicationFailures[peerID]++
		n.lastFailureTime[peerID] = time.Now()

		// Log only first few failures to avoid spam
		if n.replicationFailures[peerID] <= 3 {
			log.Printf("[%s] Replication to %s failed (count: %d): %v", n.id, peerID, n.replicationFailures[peerID], err)
		}
	} else {
		// Success: reset counters
		n.replicationFailures[peerID] = 0
		delete(n.lastFailureTime, peerID)

		// Update last ack timestamp
		n.lastHeartbeatAckMu.Lock()
		n.lastHeartbeatAck[peerID] = time.Now()
		n.lastHeartbeatAckMu.Unlock()
	}
}

func (n *Node) sendHeartbeats() {
	defer n.heartbeatRunning.Store(false) // Clear flag
	for {
		select {
		case <-n.heartbeatTimer.C:
			n.replicateToAll() // just to verify pipeline health
		case <-n.heartbeatStop:
			log.Printf("[%s] Heartbeat goroutine stopping (heartbeatStop signal)", n.id)
			return
		case <-n.shutdownCh:
			log.Printf("[%s] Heartbeat goroutine stopping (shutdown signal)", n.id)
			return
		}
	}
}
