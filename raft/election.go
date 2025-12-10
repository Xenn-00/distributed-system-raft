package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

// Deprecated: becomeFollower is deprecated. Use BecomeFollowerWithPipelining instead
func (n *Node) becomeFollower(term uint64) {
	// n.mu.Lock()
	// defer n.mu.Unlock()

	n.StopPipelinedReplication()

	if term == 0 {
		n.state = Follower   // Change node's state to follower
		n.currentTerm = term // Update current term
		n.votedFor = ""      // Reset votedFor
		n.leaderID = ""      // Clear leader when stepping down
	} else {
		// Already restored from disk
		n.state = Follower
		// Keep currentTerm, votedFor as-is
	}

	// Stop heartbeat timer if running
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
		n.heartbeatTimer = nil
	}

	n.resetElectionTimer()
	log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
}

func (n *Node) becomeCandidate() {
	// Caller should hold n.mu.lock
	// n.mu.Lock()
	// defer n.mu.Unlock()

	n.state = Candidate // Change node's state to candidate
	n.currentTerm++     // Increment current term (starting a new election)
	n.votedFor = n.id   // vote for self

	// Persist state
	if err := n.storage.SaveTerm(n.currentTerm); err != nil {
		log.Printf("[%s] Failed to save term: %v", n.id, err)
	}
	if err := n.storage.SaveVote(n.votedFor); err != nil {
		log.Printf("[%s] Failed to save vote: %v", n.id, err)
	}

	log.Printf("[%s] Became CANDIDATE at term %d", n.id, n.currentTerm)
}

// Deprecated: becomeLeader is deprecated. Use BecomeLeaderWithPipelining instead
func (n *Node) becomeLeader() {
	// Caller should hold n.mu.lock

	n.state = Leader  // Change node's state to leader
	n.leaderID = n.id // Set self as leader

	// Initialize leader state (including snapshot)
	lastLogIndex := n.getLastLogIndex()
	for peerID := range n.peers {
		if peerID == n.id {
			continue
		}
		n.nextIndex[peerID] = lastLogIndex + 1 // next log index to send to each follower
		n.matchIndex[peerID] = 0               // highest log index known to be replicated on each follower
	}

	// Stop election timer, start heartbeat timer
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.heartbeatTimer = time.NewTicker(HeartbeatInterval)

	log.Printf("[%s] Became LEADER at term %d (lastLogIndex=%d)", n.id, n.currentTerm, lastLogIndex)

	go n.StartPipelinedReplication()
	// Start sending heartbeats
	go n.sendHeartbeats()
}

func (n *Node) resetElectionTimer() {
	timeout := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))

	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		n.electionTimer.Reset(timeout)
	}
}

func (n *Node) startElection() {
	n.becomeCandidate() // Transition to candidate state
	// Prepare RequestVote RPC parameters
	n.mu.Lock()

	currentTerm := n.currentTerm
	candidateId := n.id
	// Get last log info (handles snapshot automatically)
	lastLogIndex := n.getLastLogIndex() // collecting log info from last log entry
	lastLogTerm := n.getLastLogTerm()   // collecting term info from last log entry

	if n.consecutiveElectionFailures > 0 {
		backoff := time.Duration(n.consecutiveElectionFailures) * 200 * time.Millisecond
		maxBackoff := 2 * time.Second
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		log.Printf("[%s] Election backoff: %v (failures: %d)", n.id, backoff, n.consecutiveElectionFailures)
		n.mu.Unlock()
		time.Sleep(backoff)
		n.mu.Lock()
	}

	n.mu.Unlock()

	log.Printf("[%s] Starting election for term %d", n.id, currentTerm)

	votes := 1 // vote for self
	var voteMu sync.Mutex
	// Send RequestVote RPCs to all peers
	for peerID := range n.peers {
		if peerID == n.id {
			continue
		}
		go func(peerID string) {
			voteGranted := n.requestVoteFromPeer(peerID, currentTerm, candidateId, lastLogIndex, lastLogTerm)

			if !voteGranted {
				return
			}

			// Lock only for vote counting
			voteMu.Lock()
			votes++
			currentVotes := votes
			voteMu.Unlock()

			log.Printf("[%s] Received vote from %s (%d/%d)", n.id, peerID, currentVotes, len(n.peers))

			// Lock separately for state check
			n.mu.Lock()
			majority := len(n.peers)/2 + 1
			shouldBecomeLeader := currentVotes >= majority && n.state == Candidate && n.currentTerm == currentTerm
			n.mu.Unlock()

			// Call becomeLeaderWithPipeling without holding lock
			if shouldBecomeLeader {
				n.BecomeLeaderWithPipelining()
			}
		}(peerID)
	}

	// After election, check if won
	time.AfterFunc(ElectionTimeoutMax, func() {
		n.mu.Lock()
		defer n.mu.Unlock()

		if n.state == Candidate && n.currentTerm == currentTerm {
			// Lost election
			n.consecutiveElectionFailures++
			log.Printf("[%s] Election failed (term %d), failures: %d",
				n.id, currentTerm, n.consecutiveElectionFailures)
		}
	})

	// Reset election timer
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
}

// requestVoteFromPeer seperating logic for better debugging
func (n *Node) requestVoteFromPeer(peerID string, term uint64, candidateID string, lastLogIndex, lastLogTerm uint64) bool {
	client, err := n.getClient(peerID)
	if err != nil {
		log.Printf("[%s] Failed to get client for %s: %v", n.id, peerID, err)
		return false
	}

	// Retry up to 2 times
	var resp *pb.RequestVoteResponse
	var lastErr error
	maxAttempts := 2

	for attempt := 0; attempt < maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		req := &pb.RequestVoteRequest{
			Term:         term,
			CandidateId:  candidateID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}

		resp, lastErr = client.RequestVote(ctx, req)
		cancel()

		if lastErr == nil {
			break
		}

		if attempt == 0 {
			log.Printf("[%s] RequestVote to %s failed (attempt %d): %v, retrying...",
				n.id, peerID, attempt+1, lastErr)
			time.Sleep(50 * time.Millisecond)
		}
	}

	if lastErr != nil {
		log.Printf("[%s] RequestVote to %s failed after %d attempts: %v",
			n.id, peerID, maxAttempts, lastErr)
		return false
	}

	if resp == nil {
		log.Printf("[%s] RequestVote to %s returned nil response", n.id, peerID)
		return false
	}

	// Lock for state update
	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if received higher term
	if resp.Term > n.currentTerm {
		log.Printf("[%s] Received higher term %d from %s, stepping down",
			n.id, resp.Term, peerID)

		n.currentTerm = resp.Term
		n.votedFor = ""
		n.leaderID = ""
		n.storage.SaveTerm(n.currentTerm)
		n.storage.SaveVote(n.votedFor)

		// CRITICAL: Unlock BEFORE calling becomeFollower!
		n.mu.Unlock()
		n.BecomeFollowerWithPipelining(resp.Term)

		// Re-lock to satisfy defer (will immediately unlock)
		n.mu.Lock()
		return false
	}

	// Return vote result
	return resp.VoteGranted && n.state == Candidate && n.currentTerm == term
}
