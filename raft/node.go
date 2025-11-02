package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	mu sync.Mutex

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []pb.LogEntry

	// Volatile state
	id          string
	state       NodeState
	peers       map[string]string // list of cluster members (peerID -> address)
	commitIndex uint64
	lastApplied uint64

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Channels
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	shutdownCh     chan struct{}

	// gRPC clients
	clients map[string]pb.RaftClient
}

func NewNode(id string, peers map[string]string) *Node {
	rand.Seed(time.Now().UnixNano() + int64(len(id)))
	node := &Node{
		id:          id,
		state:       Follower,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]pb.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		shutdownCh:  make(chan struct{}),
		clients:     make(map[string]pb.RaftClient),
	}

	// Initialize gRPC clients for each peer
	// for peerID, addr := range peers {
	// 	if peerID == id {
	// 		continue
	// 	}

	// 	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// 	if err != nil {
	// 		log.Printf("[%s] Failed to connect to peer %s: %v", id, peerID, err)
	// 		continue
	// 	}

	// 	node.clients[peerID] = pb.NewRaftClient(conn)
	// }

	return node
}

// Lazy get client with retry
func (n *Node) getClient(peerID string) (pb.RaftClient, error) {
	// Check if client already exists
	if client, ok := n.clients[peerID]; ok {
		return client, nil
	}

	// Create new gRPC client
	addr, ok := n.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID: %s", peerID)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s at %s: %v", peerID, addr, err)
	}

	client := pb.NewRaftClient(conn)
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
	// n.mu.Lock()
	// defer n.mu.Unlock()

	n.state = Leader // Change node's state to leader

	// Initialize leader state
	for peerID := range n.peers {
		if peerID == n.id {
			continue
		}
		n.nextIndex[peerID] = uint64(len(n.log)) + 1 // next log index to send to each follower
		n.matchIndex[peerID] = 0                     // highest log index known to be replicated on each follower
	}

	// Stop election timer, start heartbeat timer
	n.electionTimer.Stop()
	n.heartbeatTimer = time.NewTicker(HeartbeatInterval)

	log.Printf("[%s] Became LEADER at term %d", n.id, n.currentTerm)

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
				return // <- FIX: Return sebelum akses resp
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
				n.currentTerm = resp.Term
				n.becomeFollower(resp.Term)
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
			// Collecting necessary info for AppendEntries RPC
			term := n.currentTerm
			leaderID := n.id
			leaderCommit := n.commitIndex
			n.mu.Unlock()
			// Send AppendEntries RPCs (heartbeats) to all peers
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
					ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
					defer cancel()

					req := &pb.AppendEntriesRequest{
						Term:         term,
						LeaderId:     leaderID,
						PrevLogIndex: 0,                // Simplified for heartbeat
						PrevLogTerm:  0,                // Simplified for heartbeat
						Entries:      []*pb.LogEntry{}, // Empty = heartbeat => just checking attendance
						LeaderCommit: leaderCommit,
					}

					resp, err := client.AppendEntries(ctx, req)
					if err != nil {
						log.Printf("[%s] Heartbeat to %s failed: %v", n.id, peerID, err)
						return
					}
					n.mu.Lock()
					defer n.mu.Unlock()

					// Check if term is outdated
					if resp.Term > n.currentTerm {
						log.Printf("[%s] Received higher term %d from %s, stepping down", n.id, resp.Term, peerID)
						n.currentTerm = resp.Term
						n.becomeFollower(resp.Term)
					}
				}(peerID)
			}
		case <-n.shutdownCh:
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
