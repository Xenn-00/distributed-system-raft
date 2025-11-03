package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	mu sync.Mutex

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry

	// Volatile state
	id          string
	state       NodeState
	peers       map[string]string // list of cluster members (peerID -> address)
	commitIndex uint64
	lastApplied uint64

	// Leader tracking
	leaderID string

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Channels
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	shutdownCh     chan struct{}

	// gRPC clients
	clients   map[string]pb.RaftClient
	clientsMu sync.RWMutex

	// State machine (KV store)
	kvStore *kv.KVStore
}

func NewNode(id string, peers map[string]string) *Node {
	rand.Seed(time.Now().UnixNano() + int64(len(id)))
	node := &Node{
		id:          id,
		state:       Follower,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		leaderID:    "",
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		shutdownCh:  make(chan struct{}),
		clients:     make(map[string]pb.RaftClient),
		kvStore:     kv.NewKVStore(),
	}
	return node
}

// Propose: propose a new command to the cluster (only leader)
func (n *Node) Propose(cmd kv.Command) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only leader can propose
	if n.state != Leader {
		return fmt.Errorf("not leader")
	}

	// Encode command
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	// Create log entry
	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)) + 1,
		Command: cmdBytes,
	}

	// Append to local log
	n.log = append(n.log, entry)
	log.Printf("[%s] Proposed entry index=%d term=%d cmd=%+v", n.id, entry.Index, entry.Term, cmd)

	// Trigger replication (will happend on next heartbeat or immediate)
	go n.replicateToAll()

	return nil
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
		go n.replicateToPeer(peerID)
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

	// Get next index for this peer
	nextIdx := n.nextIndex[peerID]
	if nextIdx == 0 {
		nextIdx = 1
	}

	// Get entries to send
	var entries []*pb.LogEntry
	if nextIdx <= uint64(len(n.log)) {
		entries = n.log[nextIdx-1:] // Send from nextIdx to end
	}

	// Get previous log entry info for consistency check
	var prevLogIndex, prevLogTerm uint64
	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		if prevLogIndex <= uint64(len(n.log)) {
			prevLogTerm = n.log[prevLogIndex-1].Term
		}
	}

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	// Get client
	client, err := n.getClient(peerID)
	if err != nil {
		// Only log if we have entries to send (avoid spam for heartbeats)
		if len(entries) > 0 {
			log.Printf("[%s] Failed to get client for %s: %v", n.id, peerID, err)
		}
		return
	}

	// Send AppendEntries RPC
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		// Don't spam logs for heartbeat failures to down nodes
		if len(entries) > 0 {
			log.Printf("[%s] Failed to replicate to %s: %v", n.id, peerID, err)
		}
		return
	}

	// Log successful heartbeat occasionally (not every time - too noisy)
	if len(entries) == 0 {
		// Heartbeat success (log every 10th heartbeat to reduce noise)
		// Or just don't log at all
	} else {
		log.Printf("[%s] Replicated %d entries to %s", n.id, len(entries), peerID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if still leader and same term
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
			n.heartbeatTimer = nil
		}
		n.resetElectionTimer()
		log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
		return
	}

	if resp.Success {
		// Update matchIndex and nextIndex
		if len(entries) > 0 {
			lastIdx := entries[len(entries)-1].Index
			n.matchIndex[peerID] = lastIdx
			n.nextIndex[peerID] = lastIdx + 1

			log.Printf("[%s] Peer %s replicated up to index %d", n.id, peerID, lastIdx)

			// Try to update commit index
			n.updateCommitIndex()
		}
	} else {
		// Consistency check failed, decrement nextIndex and retry
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
			log.Printf("[%s] Peer %s rejected, decrementing nextIndex to %d", n.id, peerID, n.nextIndex[peerID])
			go n.replicateToPeer(peerID) // Retry
		}
	}
}

// updateCommitIndex checks if we can advance commitIndex
func (n *Node) updateCommitIndex() {
	// Caller must hold n.mu

	// Find highest N where majority has replicated
	for N := n.commitIndex + 1; N <= uint64(len(n.log)); N++ {
		if n.log[N-1].Term != n.currentTerm {
			continue // Only consider entries from current term
		}

		count := 1 // Leader itself
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= N {
				count++
			}
		}

		majority := len(n.peers)/2 + 1
		if count >= majority {
			log.Printf("[%s] Advancing commitIndex from %d to %d (majority confirmed)", n.id, n.commitIndex, N)
			n.commitIndex = N

			// Apply commited entries
			go n.applyEntries()
		} else {
			break // Can't commit higher indices yet
		}
	}
}

// applyEntries applies committed entries to state machine
func (n *Node) applyEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]

		log.Printf("[%s] Applying entry index=%d term=%d", n.id, entry.Index, entry.Term)

		if err := n.kvStore.Apply(entry.Command); err != nil {
			log.Printf("[%s] Failed to apply entry %d: %v", n.id, entry.Index, err)
		}
	}
}

// GetKvStore returns the KV store (for client queries)
func (n *Node) GetKVStore() *kv.KVStore {
	return n.kvStore
}

// Isleader checks if this node is the leader
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
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

	log.Printf("[%s] Became FOLLOWER at term 0", n.id)
	n.mu.Lock()
	n.becomeFollower(0)
	n.mu.Unlock()
	go n.run()
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

func (n *Node) becomeFollower(term uint64) {
	// n.mu.Lock()
	// defer n.mu.Unlock()

	n.state = Follower   // Change node's state to follower
	n.currentTerm = term // Update current term
	n.votedFor = ""      // Reset votedFor
	n.leaderID = ""      // Clear leader when stepping down

	// Stop heartbeat timer if running
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}

	n.resetElectionTimer()
	log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
}

func (n *Node) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Candidate // Change node's state to candidate
	n.currentTerm++     // Increment current term (starting a new election)
	n.votedFor = n.id   // vote for self

	log.Printf("[%s] Became CANDIDATE at term %d", n.id, n.currentTerm)
}

func (n *Node) becomeLeader() {
	// Caller should hold n.mu.lock

	n.state = Leader  // Change node's state to leader
	n.leaderID = n.id // Set self as leader

	// Initialize leader state
	lastLogIndex := uint64(len(n.log))
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

	// Start sending heartbeats
	go n.sendHeartbeats()

	// // Immediate first heartbeat
	// go n.replicateToAll()
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
	lastLogIndex := uint64(len(n.log)) // collecting log info from last log entry
	lastLogTerm := uint64(0)           // collecting term info from last log entry
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
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
			// Lazy get client
			client, err := n.getClient(peerID)
			if err != nil {
				log.Printf("[%s] Failed to get client for %s: %v", n.id, peerID, err)
				return
			}

			// Retry up to 2 times
			var resp *pb.RequestVoteResponse
			var lastErr error
			maxAttempts := 2
			for attempt := range maxAttempts {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

				req := &pb.RequestVoteRequest{
					Term:         currentTerm,
					CandidateId:  candidateId,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				// Send RequestVote RPC
				resp, lastErr = client.RequestVote(ctx, req)
				cancel()
				if lastErr == nil {
					break // Success
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
				return // <- FIX: Return before accessing resp
			}

			// CRITICAL: Double-check resp is not nil (defensive programming)
			if resp == nil {
				log.Printf("[%s] RequestVote to %s returned nil response", n.id, peerID)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Check if term is outdated
			if resp.Term > n.currentTerm {
				log.Printf("[%s] Received higher term %d from %s, stepping down",
					n.id, resp.Term, peerID)
				n.currentTerm = resp.Term
				n.votedFor = ""
				n.state = Follower
				n.leaderID = ""
				if n.heartbeatTimer != nil {
					n.heartbeatTimer.Stop()
				}
				n.becomeFollower(resp.Term)
				log.Printf("[%s] Became FOLLOWER at term %d", n.id, n.currentTerm)
				return
			}

			// Count votes
			if resp.VoteGranted && n.state == Candidate && n.currentTerm == currentTerm {
				voteMu.Lock()
				votes++
				currentVotes := votes
				voteMu.Unlock()

				log.Printf("[%s] Received vote from %s (%d/%d)", n.id, peerID, currentVotes, len(n.peers))

				// Check if won the election
				if currentVotes >= len(n.peers)/2 && n.state == Candidate { // majority
					n.becomeLeader()
				}
			}
		}(peerID)
	}

	// Reset election timer
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
}

func (n *Node) sendHeartbeats() {
	for {
		select {
		case <-n.heartbeatTimer.C:
			n.mu.Lock()
			if n.state != Leader { // status check
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			// Replicate to all peers (includes heartbeat + log entries if any)
			n.replicateToAll()
		case <-n.shutdownCh:
			log.Printf("[%s] Heartbeat goroutine shutting down", n.id)
			return
		}
	}
}

func (n *Node) Shutdown() {
	close(n.shutdownCh)
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
}
