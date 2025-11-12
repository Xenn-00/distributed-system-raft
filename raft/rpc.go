package raft

import (
	"context"
	"encoding/json"
	"log"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/kv"
)

// RPC handlers
// RequestVote handles incoming RequestVote RPCs.
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("[%s] Received RequestVote from %s for term %d", n.id, req.CandidateId, req.Term)

	resp := &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm
	if req.Term < n.currentTerm {
		log.Printf("[%s] Rejected vote for %s: stale term (%d < %d)", n.id, req.CandidateId, req.Term, n.currentTerm)
		return resp, nil
	}

	// CRITICAL: If I'm leader at same term, reject
	if req.Term == n.currentTerm && n.state == Leader {
		log.Printf("[%s] Rejected vote for %s: I'm leader at same term %d",
			n.id, req.CandidateId, n.currentTerm)
		return resp, nil
	}

	// if RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
	if req.Term > n.currentTerm {
		log.Printf("[%s] Received higher term %d from %s (my term: %d), stepping down",
			n.id, req.Term, req.CandidateId, n.currentTerm)

		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower
		n.leaderID = ""

		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
			n.heartbeatTimer = nil
		}

		n.storage.SaveTerm(n.currentTerm)
		n.storage.SaveVote(n.votedFor)
		n.resetElectionTimer()

		log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
	}

	// Check if candidate's log is up-to-date
	if !n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		log.Printf("[%s] Rejected vote for %s: log not up-to-date (candidate: idx=%d term=%d, mine: idx=%d term=%d)",
			n.id, req.CandidateId, req.LastLogIndex, req.LastLogTerm,
			n.getLastLogIndex(), n.getLastLogTerm())
		return resp, nil
	}

	// Grant vote if:
	// 1. Haven't voted or already voted for this candidate
	// 2. Candidate's log is at least as up-to-date as receiver's log
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		n.votedFor = req.CandidateId
		resp.VoteGranted = true

		n.storage.SaveVote(n.votedFor)
		n.resetElectionTimer()
		log.Printf("[%s] Granted vote to %s for term %d", n.id, req.CandidateId, req.Term)
	} else {
		log.Printf("[%s] Rejected vote for %s: already voted for %s", n.id, req.CandidateId, n.votedFor)
	}

	resp.Term = n.currentTerm

	return resp, nil
}

// AppendEntries handles incoming AppendEntries RPCs (heartbeats and log replication).
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm
	if req.Term < n.currentTerm {
		log.Printf("[%s] Rejected AppendEntries from %s: stale term (%d < %d)", n.id, req.LeaderId, req.Term, n.currentTerm)
		return resp, nil
	}

	// If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower

		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
			n.heartbeatTimer = nil
		}
		n.resetElectionTimer()

		log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
	}

	if n.leaderID != req.LeaderId {
		n.leaderID = req.LeaderId
		log.Printf("[%s] Recognized leader: %s at term %d", n.id, req.LeaderId, req.Term)
	}

	// Reset election timer on valid AppendEntries
	n.resetElectionTimer()

	// Heartbeat (no entries)
	if len(req.Entries) == 0 {
		// Update commitIndex from leader
		if req.LeaderCommit > n.commitIndex {
			oldCommit := n.commitIndex
			n.commitIndex = min(req.LeaderCommit, uint64(len(n.log)))
			// Only log if actually changed
			if n.commitIndex != oldCommit {
				log.Printf("[%s] Updated commitIndex from %d to %d", n.id, oldCommit, n.commitIndex)
				go n.applyEntries()
			}
			go n.applyEntries()
		}

		resp.Success = true
		return resp, nil
	}

	// Log repication - Consistency check
	if req.PrevLogIndex > 0 {
		// Check if we have entry at prevLogIndex
		if req.PrevLogIndex > uint64(len(n.log)) {
			log.Printf("[%s] Rejected AppendEntries: log too short (prevLogIndex=%d, len=%d)", n.id, req.PrevLogIndex, len(n.log))
			return resp, nil
		}

		// Check if term matches
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			log.Printf("[%s] Rejected AppendEntries: term missmatch at index %d (have %d, want %d)", n.id, req.PrevLogIndex, n.log[req.PrevLogIndex-1].Term, req.PrevLogTerm)
			return resp, nil
		}
	}

	// Consistency check passed, append entries
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1

		// If existing entry conflicts, delete it and all following
		if idx <= uint64(len(n.log)) {
			if n.log[idx-1].Term != entry.Term {
				log.Printf("[%s] Conflict at index %d, truncating log", n.id, idx)

				// Truncate on disk
				n.storage.TruncateLogFrom(idx)
				n.log = n.log[:idx-1]
			} else {
				continue // Entry already exists and matches
			}
		}

		// Persist to disk FIRST
		if err := n.storage.AppendLog(entry); err != nil {
			log.Printf("[%s] Failed to persist log entry: %v", n.id, err)
			return resp, nil
		}

		// Append new entry
		n.log = append(n.log, entry)
		log.Printf("[%s] Appended entry index=%d term=%d", n.id, entry.Index, entry.Term)
	}

	// Update commitIndex
	if req.LeaderCommit > n.commitIndex {
		oldCommit := n.commitIndex
		n.commitIndex = min(req.LeaderCommit, uint64(len(n.log)))
		// Only log if actually changed
		if n.commitIndex != oldCommit {
			log.Printf("[%s] Updated commitIndex from %d to %d", n.id, oldCommit, n.commitIndex)
			go n.applyEntries()
		}
		go n.applyEntries()
	}

	resp.Success = true
	return resp, nil
}

// InstallSnapshot handles incoming InstallSnapshot RPCs
func (n *Node) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("[%s] Received InstallSnapshot from %s: lastIncludedIndex=%d, lastIncludedTerm=%d, size=%d bytes", n.id, req.LeaderId, req.LastIncludedIndex, req.LastIncludedTerm, len(req.Data))

	resp := &pb.InstallSnapshotResponse{
		Term: n.currentTerm,
	}

	// Reply immediately if term < currentTerm
	if req.Term < n.currentTerm {
		log.Printf("[%s] Rejected InstallSnapshot: stale term (%d < %d)", n.id, req.Term, n.currentTerm)
		return resp, nil
	}

	// Convert to follower if higher term
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower
		n.leaderID = ""

		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
			n.heartbeatTimer = nil
		}

		n.storage.SaveTerm(n.currentTerm)
		n.storage.SaveVote(n.votedFor)

		log.Printf("[%s] Become FOLLOWER at term %d", n.id, n.currentTerm)
	}

	// Track leader
	if n.leaderID != req.LeaderId {
		n.leaderID = req.LeaderId
		log.Printf("[%s] Recognized leader: %s at term %d", n.id, req.LeaderId, req.Term)
	}

	n.resetElectionTimer()

	// Save snapshot to disk
	if err := n.storage.SaveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data); err != nil {
		log.Printf("[%s] Failed to save snapshot: %v", n.id, err)
		return resp, nil
	}

	// Apply snapshot to state machine
	var kvState map[string]string
	if err := json.Unmarshal(req.Data, &kvState); err != nil {
		log.Printf("[%s] Failed to unmarshal snapshot data: %v", n.id, err)
		return resp, nil
	}

	// Clear current KV store
	n.kvStore = kv.NewKVStore()
	for k, v := range kvState {
		n.kvStore.Set(k, v)
	}

	log.Printf("[%s] Applied snapshot: %d keys restored", n.id, len(kvState))

	// Discard log entries covered by snapshot
	var newLog []*pb.LogEntry
	for _, entry := range n.log {
		if entry.Index > req.LastIncludedIndex {
			newLog = append(newLog, entry)
		}
	}

	// Delete old logs from disk
	if err := n.storage.TruncateLogFrom(1); err != nil {
		log.Printf("[%s] Failed to truncate old logs: %v", n.id, err)
	}

	// Save new log
	if len(newLog) > 0 {
		if err := n.storage.AppendLogs(newLog); err != nil {
			log.Printf("[%s] Failed to save logs: %v", n.id, err)
		}
	}

	n.log = newLog
	n.lastApplied = req.LastIncludedIndex
	n.commitIndex = req.LastIncludedIndex

	log.Printf("[%s] InstallSnapshot complete: lastApplied=%d, log entries=%d", n.id, n.lastApplied, len(n.log))

	return resp, nil
}
