package raft

import (
	"context"
	"log"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
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

	// if RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.state = Follower

		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
		}
		n.resetElectionTimer()

		log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
	}

	// Grant vote if:
	// 1. Haven't voted or already voted for this candidate
	// 2. Candidate's log is at leat as up-to-date as receiver's log
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		// Simplified: assuming candidate's log is always up-to-date
		n.votedFor = req.CandidateId
		resp.VoteGranted = true
		n.resetElectionTimer()
		log.Printf("[%s] Granted vote to %s for term %d", n.id, req.CandidateId, req.Term)
	} else {
		log.Printf("[%s] Rejected vote for %s: already voted for %s", n.id, req.CandidateId, n.votedFor)
	}

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
		}
		n.resetElectionTimer()

		log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
	}

	// Reset election timer on valid AppendEntries
	n.resetElectionTimer()

	// Heartbeat received
	if len(req.Entries) == 0 {
		log.Printf("[%s] Received heartbeat from %s for term %d", n.id, req.LeaderId, req.Term)
		resp.Success = true
		return resp, nil
	}

	resp.Success = true
	return resp, nil
}
