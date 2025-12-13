package raft

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

// ProposeAsync queues a proposal with backpressure
func (n *Node) ProposalAsync(ctx context.Context, command []byte) (uint64, error) {
	// Fast path: Check if leader
	n.mu.Lock()
	if n.state != Leader {
		leaderAddress := n.GetLeaderAddress()
		n.mu.Unlock()
		return 0, &NotLeaderError{LeaderAddr: leaderAddress}
	}
	n.mu.Unlock()

	// Create request
	req := &proposalRequest{
		command: command,
		respCh:  make(chan *proposalResponse, 1), // Buffered
		ctx:     ctx,
	}

	// Try to enqueue with timeout (backpressure)
	select {
	case n.proposalQueue <- req:
		// Queue successfully, wait for response
	case <-time.After(100 * time.Millisecond):
		// Queue full, return backpressure error
		return 0, fmt.Errorf("proposal queue full (backpressure)")
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	// Wait for response from worker
	select {
	case resp := <-req.respCh:
		return resp.index, resp.err
	case <-ctx.Done():
		go func() {
			// Drain response channel to prevent goroutine leak
			select {
			case <-req.respCh:
				// Drained
			case <-time.After(5 * time.Second):
				// Worker never responded (likely hung)
				log.Printf("[%s] WARNING: Proposal worker did not respond after context cancel",
					n.id)
			}
		}()
		return 0, ctx.Err()
	}
}

// Propose: propose a new command to the cluster (only leader) -> transform into proposeInternal
func (n *Node) proposeInternal(ctx context.Context, command []byte) (uint64, error) {
	n.mu.Lock()

	// Only leader can propose
	if n.state != Leader {
		n.mu.Unlock()
		return 0, fmt.Errorf("not leader")
	}

	// Get last log index (handles snapshot!)
	lastLogIndex := n.getLastLogIndex()
	index := lastLogIndex + 1
	// Create log entry
	entry := &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   index,
		Command: command,
	}

	n.mu.Unlock() // release before disk I/O

	// Persist to WAL FIRST
	if err := n.storage.AppendLog(entry); err != nil {
		return 0, fmt.Errorf("failed to persiste log: %v", err)
	}

	n.mu.Lock()
	// Then append to memory
	n.log = append(n.log, entry)

	// Sample logging
	if shouldLog(index, 10) {
		log.Printf("[%s] Proposed entry index=%d term=%d", n.id, entry.Index, entry.Term)
	}

	// Unlock before waiting
	n.mu.Unlock()

	// Trigger replication (will happend on next heartbeat or immediate)
	n.triggerReplication() // instead of using go n.replicateToAll()

	// Wait for commit before returning
	if err := n.waitForCommit(ctx, index); err != nil {
		return 0, fmt.Errorf("failed to commit: %v", err)
	}

	// Check if applied after commit
	n.mu.Lock()
	applied := n.lastApplied >= index
	n.mu.Unlock()

	// Sample logging
	if shouldLog(index, 10) {
		log.Printf("[%s] Entry %d committed successfully (lastApplied=%d, applied=%v)", n.id, index, n.lastApplied, applied)
	}
	return index, nil
}

// ProcessProposals is the single dispatcher goroutine
func (n *Node) ProcessProposals() {
	for {
		select {
		case req := <-n.proposalQueue:
			// Acquire semaphore (blocks if 100 in-flight)
			select {
			case n.ProposalSem <- struct{}{}:
				// Got semaphore, spawn worker
				go n.handleProposal(req)
			case <-n.proposalStop:
				// Shutdown requested, reject this proposal
				req.respCh <- &proposalResponse{
					err: fmt.Errorf("node shutting down"),
				}
				return
			}
		case <-n.proposalStop:
			log.Printf("[%s] Proposal processor stopping", n.id)
			return
		}
	}
}

// handleProposal processes a single proposal (runs in goroutine)
func (n *Node) handleProposal(req *proposalRequest) {
	// Critical: always release semaphore
	defer func() { <-n.ProposalSem }()

	// Check context first (might be cancelled)
	select {
	case <-req.ctx.Done():
		req.respCh <- &proposalResponse{err: req.ctx.Err()}
		return
	default:
	}

	// Call internal propose logic
	index, err := n.proposeInternal(req.ctx, req.command)

	// Send response back (non-blocking)
	select {
	case req.respCh <- &proposalResponse{index: index, err: err}:
		// response sent
	case <-time.After(100 * time.Millisecond):
		// Client gave up, log and move on
		log.Printf("[%s] Client abandoned proposal for index %d", n.id, index)
	}
}
