package raft

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

const (
	// Pipelining parameters
	MaxInFlightRPCs = 10 // Max concurrent RPCs per peer
	RPCTimeout      = 1 * time.Second
)

// PeerReplicator manages pipelined replication to a single peer
type PeerReplicator struct {
	peerID    string
	node      *Node
	inflightQ chan *replicationTask // Queue of in-flight tasks
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// replicationTask represents a single AppendEntries RPC
type replicationTask struct {
	req    *pb.AppendEntriesRequest
	respCh chan *replicationResponse
	sentAt time.Time
}

// replicationResponse captures RPC result
type replicationResponse struct {
	success    bool
	higherTerm uint64
	matchIndex uint64
	err        error
}

// newPeerReplicator creates a new pipelined replicator for a peer
func newPeerReplicator(peerID string, node *Node) *PeerReplicator {
	return &PeerReplicator{
		peerID:    peerID,
		node:      node,
		inflightQ: make(chan *replicationTask, MaxInFlightRPCs),
		stopCh:    make(chan struct{}),
	}
}

// start begins pipelined replication
func (pr *PeerReplicator) start() {
	pr.wg.Add(1)
	go pr.sendLoop()

	pr.wg.Add(1)
	go pr.receiveLoop()
}

// stop gracefully shutdown the replicator
func (pr *PeerReplicator) stop() {
	close(pr.stopCh)

	// Wait with timeout (prevent infinite hang)
	done := make(chan struct{})
	go func() {
		pr.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(5 * time.Second):
		// Force shutdown after timeout
		log.Printf("[%s] WARNING: Replicator for %s did not stop gracefully after 5s", pr.node.id, pr.peerID)
	}
}

// sendLoop continously sends AppendEntries RPCs
func (pr *PeerReplicator) sendLoop() {
	defer pr.wg.Done()

	ticker := time.NewTicker(50 * time.Millisecond) // check every 50ms
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pr.maybeSendBatch()
		case <-pr.stopCh:
			log.Printf("[%s] Pipeline sendLoop STOPPING for peer %s", pr.node.id, pr.peerID)
			return
		}
	}
}

// maybeSendBatch sends a batch of entries if needed
func (pr *PeerReplicator) maybeSendBatch() {
	pr.node.mu.Lock()

	if pr.node.state != Leader {
		pr.node.mu.Unlock()
		return
	}

	// Check if there's work to do
	nextIdx := pr.node.nextIndex[pr.peerID]
	// lastLogIndex := pr.node.getLastLogIndex()

	// if nextIdx > lastLogIndex {
	// 	pr.node.mu.Unlock()
	// 	return // No new entries
	// }

	// Check in-flight limit
	if len(pr.inflightQ) >= MaxInFlightRPCs {
		pr.node.mu.Unlock()
		return // Too many in-flight
	}

	// Prepare request
	req := pr.node.prepareAppendEntriesRequest(nextIdx)
	term := pr.node.currentTerm

	pr.node.mu.Unlock()

	// // Log heartbeat sends
	// if len(req.Entries) == 0 {
	// 	// This is a heartbeat
	// 	log.Printf("[%s] Sending heartbeat to %s (term=%d, commitIndex=%d)",
	// 		pr.node.id, pr.peerID, term, req.LeaderCommit)
	// }

	// Create task
	task := &replicationTask{
		req:    req,
		respCh: make(chan *replicationResponse, 1),
		sentAt: time.Now(),
	}

	// Send RPC asynchronously
	go pr.sendRPC(task, term)

	// Track in-flight
	select {
	case pr.inflightQ <- task:
	default:
		// Queue full (shouldn't happen. but be defensive)
		log.Printf("[%s] In-flight queue full for %s", pr.node.id, pr.peerID)
	}
}

// sendRPC sends the AppendEntries RPC
func (pr *PeerReplicator) sendRPC(task *replicationTask, term uint64) {
	client, err := pr.node.getClient(pr.peerID)
	if err != nil {
		task.respCh <- &replicationResponse{err: err}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
	defer cancel()

	resp, err := client.AppendEntries(ctx, task.req)
	if err != nil {
		task.respCh <- &replicationResponse{err: err}
		return
	}

	// Calculate matchIndex from request
	var matchIndex uint64
	if len(task.req.Entries) > 0 {
		matchIndex = task.req.Entries[len(task.req.Entries)-1].Index
	}

	// Check for higher term
	if resp.Term > term {
		task.respCh <- &replicationResponse{
			higherTerm: resp.Term,
		}
		return
	}

	task.respCh <- &replicationResponse{
		success:    resp.Success,
		matchIndex: matchIndex,
	}
}

// receiveLo0p processes responses
func (pr *PeerReplicator) receiveLoop() {
	defer pr.wg.Done()

	log.Printf("[%s] Pipeline receiveLoop STARTED for peer %s", pr.node.id, pr.peerID)

	for {
		select {
		case task := <-pr.inflightQ:
			// Wait for response
			select {
			case resp := <-task.respCh:
				pr.handleResponse(task, resp)
			case <-time.After(2 * RPCTimeout):
				// Timeout - treat as failure
				pr.handleResponse(task, &replicationResponse{
					err: context.DeadlineExceeded,
				})
			case <-pr.stopCh:
				// Shutdown requested, drain this task then exit
				log.Printf("[%s] Replicator for %s stopping (draining task)",
					pr.node.id, pr.peerID)
				return
			}
		case <-pr.stopCh:
			// Shutdown requested
			log.Printf("[%s] Replicator for %s stopped", pr.node.id, pr.peerID)
			return
		}
	}
}

// handleResponse processes a single RPC response
func (pr *PeerReplicator) handleResponse(task *replicationTask, resp *replicationResponse) {
	pr.node.mu.Lock()
	// Check if still leader
	if pr.node.state != Leader {
		pr.node.mu.Unlock()
		return
	}
	pr.node.mu.Unlock()

	// Handle higher term
	if resp.higherTerm > 0 {
		pr.node.mu.Lock()

		log.Printf("[%s] Stepping down: received higher term %d", pr.node.id, resp.higherTerm)
		pr.node.currentTerm = resp.higherTerm
		pr.node.votedFor = ""
		pr.node.leaderID = ""
		pr.node.storage.SaveTerm(pr.node.currentTerm)
		pr.node.storage.SaveVote(pr.node.votedFor)

		// Unlock before calling becomeFollower
		pr.node.mu.Unlock()
		pr.node.BecomeFollowerWithPipelining(resp.higherTerm)
		return
	}

	pr.node.mu.Lock()
	defer pr.node.mu.Unlock()
	// Handle error
	if resp.err != nil {
		pr.node.replicationFailures[pr.peerID]++
		pr.node.lastFailureTime[pr.peerID] = time.Now()
		return
	}

	// Handle success
	if resp.success {
		pr.node.lastHeartbeatAckMu.Lock()
		pr.node.lastHeartbeatAck[pr.peerID] = time.Now()
		pr.node.lastHeartbeatAckMu.Unlock()
		pr.node.replicationFailures[pr.peerID] = 0
		delete(pr.node.lastFailureTime, pr.peerID)

		if resp.matchIndex > 0 {
			pr.node.matchIndex[pr.peerID] = resp.matchIndex
			pr.node.nextIndex[pr.peerID] = resp.matchIndex + 1

			latency := time.Since(task.sentAt)
			if shouldLog(resp.matchIndex, 10) {
				log.Printf("[%s] Peer %s replicate up to %d (latency: %v)", pr.node.id, pr.peerID, resp.matchIndex, latency)
			}

			pr.node.updateCommitIndexWithBatching()
		}
	} else {
		// Consistency check failed, decrement nextIndex
		if pr.node.nextIndex[pr.peerID] > 1 {
			pr.node.nextIndex[pr.peerID]--
		}
	}
}

// startPipelinedReplication initializes replicators for all peers
func (np *Node) StartPipelinedReplication() {
	np.replicatorsMu.Lock()
	defer np.replicatorsMu.Unlock()

	for peerID := range np.peers {
		if peerID == np.id {
			continue
		}

		replicator := newPeerReplicator(peerID, np)
		replicator.start()
		np.replicators[peerID] = replicator
	}

	log.Printf("[%s] Started pipelined replication to %d peers", np.id, len(np.replicators))
}

// stopPipelinedReplication shutdown all replications
func (np *Node) StopPipelinedReplication() {
	// Lock only to read/clear replicators map
	np.replicatorsMu.Lock()

	replicatorsCopy := make([]*PeerReplicator, 0, len(np.replicators))
	for _, replicator := range np.replicators {
		replicatorsCopy = append(replicatorsCopy, replicator)
	}

	// Clear map immediately
	np.replicators = make(map[string]*PeerReplicator)
	np.replicatorsMu.Unlock()

	// Stop all replicators without holding any locks
	for _, replicator := range replicatorsCopy {
		replicator.stop()
	}

	log.Printf("[%s] Stopped pipelined replication", np.id)
}

// Modified becomeLeader to start pipelined replication
func (np *Node) BecomeLeaderWithPipelining() {
	// Stop old resources before acquiring lock
	np.StopPipelinedReplication()
	// np.stopHeartbeat()

	np.mu.Lock()
	// Double-check still candidate (might have stepped down)
	if np.state != Candidate {
		np.mu.Unlock()
		log.Printf("[%s] Not candidate anymore, aborting becomeLeader", np.id)
		return
	}

	// Caller should hold Lock
	np.state = Leader
	np.leaderID = np.id

	np.consecutiveElectionFailures = 0 // reset counter

	// initialized leader state
	lastLogIndex := np.getLastLogIndex()
	for peerID := range np.peers {
		if peerID == np.id {
			continue
		}

		np.nextIndex[peerID] = lastLogIndex + 1
		np.matchIndex[peerID] = 0
	}

	if np.electionTimer != nil {
		np.electionTimer.Stop()
	}
	// safe cleanup before recreating
	np.safeStopTimer(np.electionTimer, "election", np.id)

	np.heartbeatStop = make(chan struct{})
	np.heartbeatTimer = time.NewTicker(HeartbeatInterval)
	np.heartbeatRunning.Store(true) // Mark as running
	log.Printf("[%s] Became LEADER at term %d (pipelined mode)", np.id, np.currentTerm)
	np.mu.Unlock()

	// Start pipelined replication
	go np.StartPipelinedReplication()
	go np.sendHeartbeats()
}

// Modified becomeFollower to stop replication
func (np *Node) BecomeFollowerWithPipelining(term uint64) {
	log.Printf("[%s] Transitioning to FOLLOWER (term %d)", np.id, term)
	np.StopPipelinedReplication()
	// stop heartbeat goroutine
	np.stopHeartbeat()

	if term > 0 {
		np.currentTerm = term
		np.votedFor = ""
	}

	np.state = Follower
	np.leaderID = ""

	// safe cleanup
	np.safeStopTicker(np.heartbeatTimer, "heartbeat", np.id)

	np.resetElectionTimer()

	log.Printf("[%s] Became FOLLOWER at term %d", np.id, np.currentTerm)
}

// stopHeartBeat safely stops the heartbeat goroutine
func (np *Node) stopHeartbeat() {
	// Close heartbeat channel to signal goroutine to stop
	if np.heartbeatRunning.CompareAndSwap(true, false) {
		// Successfully transitioned from running to stopped
		if np.heartbeatStop != nil {
			select {
			case np.heartbeatStop <- struct{}{}:
				log.Printf("[%s] Sent heartbeat stop signal", np.id)
			case <-time.After(100 * time.Millisecond):
				log.Printf("[%s] WARNING: heartbeat stop signal timeout", np.id)
			}
		}
	} else {
		log.Printf("[%s] Heartbeat already stopped", np.id)
	}

	if np.heartbeatTimer != nil {
		np.heartbeatTimer.Stop()
		np.heartbeatTimer = nil
	}
}
