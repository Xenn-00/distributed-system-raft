package raft

import "github.com/Xenn-00/distributed-kv-store/kv"

func (n *Node) HasSnapshot() bool {
	return n.storage.HasSnapshot()
}

func (n *Node) GetSnapshotInfo() (uint64, uint64) {
	lastIdx, lastTerm, _, _ := n.storage.LoadSnapshot()
	return lastIdx, lastTerm
}

func (n *Node) GetLogSize() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log)
}

func (n *Node) getLastLogIndex() uint64 {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Index
	}

	// Check snapshot
	if n.storage.HasSnapshot() {
		snapIndex, _, _, _ := n.storage.LoadSnapshot()
		return snapIndex
	}

	return 0
}

func (n *Node) getLastLogTerm() uint64 {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Term
	}

	// Check snapshot
	if n.storage.HasSnapshot() {
		_, snapTerm, _, _ := n.storage.LoadSnapshot()
		return snapTerm
	}

	return 0
}

// GetKvStore returns the KV store (for client queries)
func (n *Node) GetKVStore() *kv.KVStore {
	return n.kvStore
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as ours
// If the logs have last entries with different terms, then the log with the later term is
// more up-to-date. If the logs end with the same term, then whichever log is longer is
// more up-to-date
func (n *Node) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	// get our last log info
	lastLogIndex := n.getLastLogIndex()
	lastLogTerm := n.getLastLogTerm()

	// Candidate's log is more up-to-date if:
	// 1. Last term is higher, OR
	// 2. Same term but longer log
	if candidateLastLogTerm != lastLogTerm {
		return candidateLastLogTerm > lastLogTerm
	}
	return candidateLastLogIndex >= lastLogIndex
}

// GetLeaderID returns current leader ID (or empty if unknown)
func (n *Node) GetLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	// This is simplified - in real implementation you'd track last known leader
	if n.state == Leader {
		return n.id
	}

	return n.leaderID // Return tracked leader
}

// GetLeaderAddress return address leader (perfect for redirect)
func (n *Node) GetLeaderAddress() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	leaderID := n.leaderID
	if n.state == Leader {
		leaderID = n.id
	}

	if leaderID == "" {
		return ""
	}

	addr, ok := n.peers[leaderID]
	if !ok {
		return ""
	}

	return addr
}
