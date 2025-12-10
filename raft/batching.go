package raft

import (
	"fmt"
	"log"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

const (
	// Batching parameters
	MaxBatchSize = 50                    // Max entries per batch
	BatchTimeout = 10 * time.Millisecond // Max wait time
	MinBatchSize = 1                     // Min entries to trigger early flush
)

// proposalBatch groups multiple proposals together
type proposalBatch struct {
	requests []*proposalRequest
	entries  []*pb.LogEntry
}

// processProposalWithBatching replaces the old processProposals
// If something goes wrong, then just rollback to processProposals 🙃
func (n *Node) processProposalWithBatching() {
	ticker := time.NewTicker(BatchTimeout)
	defer ticker.Stop()

	currentBatch := &proposalBatch{
		requests: make([]*proposalRequest, 0, MaxBatchSize),
		entries:  make([]*pb.LogEntry, 0, MaxBatchSize),
	}

	for {
		select {
		case req := <-n.proposalQueue:
			// Add to current batch
			currentBatch.requests = append(currentBatch.requests, req)

			// Check if batch is full
			if len(currentBatch.requests) >= MaxBatchSize {
				// flush
				n.flushBatch(currentBatch)
				currentBatch = &proposalBatch{
					requests: make([]*proposalRequest, 0, MaxBatchSize),
					entries:  make([]*pb.LogEntry, 0, MaxBatchSize),
				}
				ticker.Reset(BatchTimeout) // Reset timer
			}
		case <-ticker.C:
			// Timeout reached, flush whatever we have
			if len(currentBatch.requests) > 0 {
				n.flushBatch(currentBatch)
				currentBatch = &proposalBatch{
					requests: make([]*proposalRequest, 0, MaxBatchSize),
					entries:  make([]*pb.LogEntry, 0, MaxBatchSize),
				}
			}

		case <-n.proposalStop:
			// Flush remaining match before shutdown
			if len(currentBatch.requests) > 0 {
				n.flushBatch(currentBatch)
			}
			log.Printf("[%s] Batching processor stopping", n.id)
			return
		}
	}
}

// flushBatch processes an entire batch of proposals at once
func (n *Node) flushBatch(batch *proposalBatch) {
	if len(batch.requests) == 0 {
		return
	}

	n.mu.Lock()

	// Check if still leader
	if n.state != Leader {
		n.mu.Unlock()
		// Reject all proposals in batch
		for _, req := range batch.requests {
			req.respCh <- &proposalResponse{
				err: fmt.Errorf("not leader"),
			}
		}
		return
	}

	// Create log entries for entire batch
	lastLogIndex := n.getLastLogIndex()
	startIndex := lastLogIndex + 1

	for i, req := range batch.requests {
		index := startIndex + uint64(i)
		entry := &pb.LogEntry{
			Term:    n.currentTerm,
			Index:   index,
			Command: req.command,
		}
		batch.entries = append(batch.entries, entry)
	}

	term := n.currentTerm
	n.mu.Unlock()

	// Persist entrie batch to WAL (singe fsync)
	if err := n.storage.AppendLogBatch(batch.entries); err != nil {
		log.Printf("[%s] Failed to persist batch: %v", n.id, err)
		// Reject all proposals
		for _, req := range batch.requests {
			req.respCh <- &proposalResponse{
				err: fmt.Errorf("failed to persist: %v", err),
			}
		}

		return
	}

	// Append to memory log
	n.mu.Lock()
	n.log = append(n.log, batch.entries...)
	n.mu.Unlock()

	log.Printf("[%s] Flushed batch of %d entries (index %d-%d)", n.id, len(batch.entries), startIndex, startIndex+uint64(len(batch.entries))-1)

	// Triger replication (all entries will be sent together)
	n.triggerReplication() // instead of using go n.replicateToAll()

	// Wait for each entry to commit individually
	for i, req := range batch.requests {
		entry := batch.entries[i]

		// reuse existing semaphore to limit goroutines
		select {
		case n.ProposalSem <- struct{}{}:
			go func(r *proposalRequest, idx uint64) {
				defer func() {
					<-n.ProposalSem
				}()
				n.waitAndRespond(r, idx, term)
			}(req, entry.Index)
		default:
			// semaphore full, wait synchronously
			n.waitAndRespond(req, entry.Index, term)
		}
	}
}
