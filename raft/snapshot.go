package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

// maybeSnapshot checks if we should create a snapshot or not
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Don't snapshot if log is small
	if len(n.log) < SnapshotThreshold {
		return
	}

	// Don't snapshot if nothing committed yet
	if n.lastApplied == 0 {
		return
	}

	// Check how many entries since last snapshot
	entriesSinceSnapshot := n.lastApplied - n.lastSnapshotIndex
	if entriesSinceSnapshot < SnapshotThreshold {
		return
	}

	log.Printf("[%s] Creating snapshot (size trigger): entries=%d, threshold=%d", n.id, entriesSinceSnapshot, SnapshotThreshold)

	// Create snapshot
	if err := n.createSnapshot(); err != nil {
		log.Printf("[%s] Failed to create snapshot: %v", n.id, err)
	}

	// Update tracking
	n.lastSnapshotTime = time.Now()
	n.lastSnapshotIndex = n.lastApplied
}

// maybeSnapshotByTime checks if we should create a snapshot (time-based)
func (n *Node) maybeSnapshotByTime() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Don't snapshot if nothing committe
	if n.lastApplied == 0 {
		return
	}

	// Don't snapshot if too few entries
	entriesSinceSnapshot := n.lastApplied - n.lastSnapshotIndex
	if entriesSinceSnapshot < MinEntriesForSnapshot {
		log.Printf("[%s] Skipping time-based snapshot: too few entries (%d < %d)", n.id, entriesSinceSnapshot, MinEntriesForSnapshot)
		return
	}

	// Check time elapsed
	timeSinceSnapshot := time.Since(n.lastSnapshotTime)
	if timeSinceSnapshot < SnapshotInterval {
		return
	}

	log.Printf("[%s] Creating snapshot (time trigger): elapsed=%v, entries=%d", n.id, timeSinceSnapshot, entriesSinceSnapshot)

	if err := n.createSnapshot(); err != nil {
		log.Printf("[%s] Failed to create snapshot: %v", n.id, err)
		return
	}

	// Update tracking
	n.lastSnapshotTime = time.Now()
	n.lastSnapshotIndex = n.lastApplied
}

// createSnapshot saves current state and truncates log
func (n *Node) createSnapshot() error {
	// Get last applied log entry info
	lastIndex := n.lastApplied
	if lastIndex == 0 {
		return fmt.Errorf("no entries to snapshot")
	}

	// Find the log entry
	var lastTerm uint64
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Index == lastIndex {
			lastTerm = n.log[i].Term
			break
		}
	}

	// Get current KV state
	kvState := n.kvStore.GetAll()

	// Serialize KV state
	data, err := json.Marshal(kvState)
	if err != nil {
		return fmt.Errorf("failed to marshal KV state: %v", err)
	}

	// Save snapshot to disk
	if err := n.storage.SaveSnapshot(lastIndex, lastTerm, data); err != nil {
		return fmt.Errorf("failed to save snapshot: %v", err)
	}

	log.Printf("[%s] Snapshot saved: lastIndex=%d, lastTerm=%d, size=%d bytes", n.id, lastIndex, lastTerm, len(data))

	// Truncate log entries before lastIndex
	var newLog []*pb.LogEntry
	for _, entry := range n.log {
		if entry.Index > lastIndex {
			newLog = append(newLog, entry)
		}
	}

	// Delete old log entries from disk
	if err := n.storage.TruncateLogFrom(1); err != nil {
		log.Printf("[%s] Failed to truncate old logs: %v", n.id, err)
	}

	// Save new log to disk
	if len(newLog) > 0 {
		if err := n.storage.AppendLogs(newLog); err != nil {
			log.Printf("[%s] Failed to save new logs: %v", n.id, err)
		}
	}

	// Update in-memory log
	oldLogSize := len(n.log)
	n.log = newLog

	log.Printf("[%s] Log compacted: %d -> %d entries (saved %d entries)", n.id, oldLogSize, len(n.log), oldLogSize-len(n.log))

	log.Printf("[%s] Snapshot created: lastIndex=%d, lastTerm=%d, size=%d bytes",
		n.id, lastIndex, lastTerm, len(data))

	return nil
}

// periodicSnapshotCheck checks for snapshot based on time
func (n *Node) periodicSnapshotCheck() {
	ticker := time.NewTicker(SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.maybeSnapshotByTime()
		case <-n.shutdownCh:
			return
		}
	}
}

// sendSnapshot sends InstallSnapshot RPC to a peer
func (n *Node) sendSnapshot(peerID string) {
	n.mu.Lock()

	// Check if we have a snapshot
	if !n.storage.HasSnapshot() {
		log.Printf("[%s] No snapshot available to send to %s", n.id, peerID)
		n.mu.Unlock()
		return
	}

	// Load snapshot from disk
	lastIncludedIndex, lastIncludedTerm, data, err := n.storage.LoadSnapshot()
	if err != nil {
		log.Printf("[%s] Failed to load snapshot: %v", n.id, err)
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	leaderID := n.id
	n.mu.Unlock()

	// Get client
	client, err := n.getClient(peerID)
	if err != nil {
		return
	}

	// Send InstallSnapshot RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Longer timeout for snapshot
	defer cancel()

	req := &pb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          leaderID,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}

	log.Printf("[%s] Sending snapshot to %s: lastIncludedIndex=%d, size=%d bytes", n.id, peerID, lastIncludedIndex, len(data))

	resp, err := client.InstallSnapshot(ctx, req)
	if err != nil {
		log.Printf("[%s] InstallSnapshot to %s failed: %v", n.id, peerID, err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if still leader
	if n.state != Leader || n.currentTerm != term {
		return
	}

	// Handle response
	if resp.Term > n.currentTerm {
		log.Printf("[%s] Stepping down: peer %s has higher term %d", n.id, peerID, resp.Term)
		n.currentTerm = resp.Term
		n.votedFor = ""
		n.state = Follower
		n.leaderID = ""
		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
		}

		n.resetElectionTimer()
		return
	}

	// update nextIndex and matchIndex
	n.nextIndex[peerID] = lastIncludedIndex + 1
	n.matchIndex[peerID] = lastIncludedIndex

	log.Printf("[%s] Snapshot sent to %s successfully, updated nextIndex to %d", n.id, peerID, n.nextIndex[peerID])
}
