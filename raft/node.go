package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/kv"
	"github.com/Xenn-00/distributed-kv-store/storage"
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

	// Storage
	storage *storage.BadgerStorage

	// Snapshot tracking
	lastSnapshotTime  time.Time
	lastSnapshotIndex uint64

	// Rate limiting for failed replications
	replicationFailures map[string]int       // peerID -> consecutive failures
	lastFailureTime     map[string]time.Time // peerID -> last failure time
}

const (
	// Snapshot ever N log entries
	SnapshotThreshold = 10 // Set low for testing, production could use 10000+

	// Time-based trigger
	SnapshotInterval = 3 * time.Minute

	// Minimum entries before time-based snapshot
	MinEntriesForSnapshot = 5 // Don't snapshot if <5 entries
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
		replicationFailures: make(map[string]int),
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
		log.Printf("[%s] Replaying %d log entries after snapshot", n.id, len(n.log))

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
				log.Printf("[%s] Replayed entry index=%d", n.id, entry.Index)
			}
		}
	}
	log.Printf("[%s] Replay complete: lastApplied=%d", n.id, n.lastApplied)
	return nil
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

	// Get last log index (handles snapshot!)
	lastLogIndex := n.getLastLogIndex()
	// Create log entry
	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   lastLogIndex + 1,
		Command: cmdBytes,
	}

	// Persist to WAL FIRST
	if err := n.storage.AppendLog(entry); err != nil {
		return fmt.Errorf("failed to persiste log: %v", err)
	}

	// Then append to memory
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

	// Get last log index
	var lastLogIndex uint64
	if len(n.log) > 0 {
		lastLogIndex = n.log[len(n.log)-1].Index
	} else {
		lastLogIndex = firstLogIndex - 1
	}

	// Get entries to send
	var entries []*pb.LogEntry
	if nextIdx <= lastLogIndex {
		// Find entries from nextIdx onwards
		for _, entry := range n.log {
			if entry.Index >= nextIdx {
				entries = append(entries, entry)
			}
		}
	}

	// Get previous log entry info for consistency check
	var prevLogIndex, prevLogTerm uint64
	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1

		// Check if prevLogIndex is in snapshot
		if n.storage.HasSnapshot() {
			snapIndex, snapTerm, _, _ := n.storage.LoadSnapshot()
			if prevLogIndex == snapIndex {
				// prevLogIndex is in snapshot, use snapshot's term
				prevLogTerm = snapTerm
				prevLogIndex = snapIndex
			} else if prevLogIndex < snapIndex {
				// Peer is too far behind, should have sent snapshot
				log.Printf("[%s] ERROR: prevLogIndex %d is before snapshot %d", n.id, prevLogIndex, snapIndex)
				n.mu.Unlock()
				n.sendSnapshot(peerID)
				return
			}
		}

		// Check if prevLogIndex is in log
		if prevLogTerm == 0 {
			for _, entry := range n.log {
				if entry.Index == prevLogIndex {
					prevLogTerm = entry.Term
					break
				}
			}
		}

		// If still not found, something wrong
		if prevLogTerm == 0 && prevLogIndex > 0 {
			// This shouldn't happen if snapshot logic is correct
			// But let's be defensive - send empty heartbeat
			log.Printf("[%s] ERROR: Cannot find prevLogTerm for index %d", n.id, prevLogIndex)
			n.mu.Unlock()
			// Don't return - let it fail gracefully on follower side
		}
	}

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	// Get client
	client, err := n.getClient(peerID)
	if err != nil {
		// Mark failure
		n.mu.Lock()
		n.replicationFailures[peerID]++
		n.lastFailureTime[peerID] = time.Now()
		n.mu.Unlock()

		// Only log first few failures
		if n.replicationFailures[peerID] <= 3 {
			log.Printf("[%s] Failed to get client for %s: %v (failure %d)", n.id, peerID, err, n.replicationFailures[peerID])
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
		// Mark failure
		n.mu.Lock()
		n.replicationFailures[peerID]++
		n.lastFailureTime[peerID] = time.Now()
		n.mu.Unlock()

		// Only log first few failures
		if len(entries) > 0 && n.replicationFailures[peerID] <= 3 {
			log.Printf("[%s] Failed to replicate to %s: %v (failure %d)", n.id, peerID, err, n.replicationFailures[peerID])
		}
		return
	}

	// Log successful heartbeat occasionally (not every time - too noisy)
	if len(entries) == 0 {
		// Heartbeat success (log every 10th heartbeat to reduce noise)
		// Or just don't log at all
	} else {
		n.mu.Lock()
		n.replicationFailures[peerID] = 0
		delete(n.lastFailureTime, peerID)
		n.mu.Unlock()
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

		// Persist state change
		n.storage.SaveTerm(n.currentTerm)
		n.storage.SaveVote(n.votedFor)
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
		}
	}
}

// updateCommitIndex checks if we can advance commitIndex
func (n *Node) updateCommitIndex() {
	// Caller must hold n.mu

	lastLogIndex := n.getLastLogIndex()

	// Find highest N where majority has replicated
	for N := n.commitIndex + 1; N <= lastLogIndex; N++ {
		// Find entry at index N
		var entryTerm uint64
		for _, entry := range n.log {
			if entry.Index == N {
				entryTerm = entry.Term
				break
			}
		}

		// Only commit entries from current term (Raft safety)
		if entryTerm != n.currentTerm {
			continue
		}

		count := 1 // Leader itself
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= N {
				count++
			}
		}

		majority := len(n.peers)/2 + 1
		if count >= majority {
			log.Printf("[%s] Advancing commitIndex from %d to %d (majority confirmed: %d/%d)", n.id, n.commitIndex, N, count, len(n.peers))
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
	for {
		n.mu.Lock()

		// Check if there's work to do
		if n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			return
		}

		nextIndex := n.lastApplied + 1

		// Find entry by Index
		var entry *pb.LogEntry
		for _, e := range n.log {
			if e.Index == nextIndex {
				entry = e
				break
			}
		}

		if entry == nil {
			// Entry not in log - check if it's in snapshot
			if n.storage.HasSnapshot() {
				snapIndex, _, _, _ := n.storage.LoadSnapshot()
				if nextIndex <= snapIndex {
					// Entry is in snapshot, already applied
					log.Printf("[%s] Entry %d is in snapshot (snapIndex=%d), skipping", n.id, nextIndex, snapIndex)
					n.lastApplied = nextIndex
					n.mu.Unlock()
					continue
				}
			}
			// Entry not found and not in snapshot - this is a bug!
			log.Printf("[%s] CRITICAL: Entry at index %d not found (lastApplied=%d, commitIndex=%d, log.len=%d)", n.id, nextIndex, n.lastApplied, n.commitIndex, len(n.log))
			n.mu.Unlock()
			return
		}

		// Found entry, copy data
		entryIndex := entry.Index
		entryCommand := make([]byte, len(entry.Command))
		copy(entryCommand, entry.Command)

		n.mu.Unlock()

		if err := n.kvStore.Apply(entry.Command); err != nil {
			log.Printf("[%s] Failed to apply entry %d: %v", n.id, entry.Index, err)
		}

		n.mu.Lock()
		if entryIndex > n.lastApplied {
			n.lastApplied = entry.Index
		}
		n.mu.Unlock()
	}
}

// Isleader checks if this node is the leader
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
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

func (n *Node) becomeFollower(term uint64) {
	// n.mu.Lock()
	// defer n.mu.Unlock()

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

	// Send Immediate heartbeat to estabilish authority
	go func() {
		n.mu.Lock()
		if n.state == Leader {
			log.Printf("[%s] Sending immediate heartbeat to estabilish leadership", n.id)
			n.mu.Unlock()
			n.replicateToAll()
		} else {
			n.mu.Unlock()
		}
	}()

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
	// if lastLogIndex > 0 {
	// 	lastLogTerm = n.log[lastLogIndex-1].Term
	// }
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
			// n.mu.Lock()
			// if n.state != Leader { // status check
			// 	n.mu.Unlock()
			// 	return
			// }
			// n.mu.Unlock()

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
	if n.storage != nil {
		n.storage.Close()
	}
}
