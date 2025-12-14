package raft

import (
	"context"
	"log"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

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
			oldCommit := n.commitIndex
			n.commitIndex = N

			// Notify all waiters for indices up to N
			for idx := oldCommit + 1; idx <= N; idx++ {
				n.NotifyCommitWaiters(idx)
			}

			// Sample logging
			if shouldLog(N, 10) {
				log.Printf("[%s] Advancing commitIndex from %d to %d (majority confirmed: %d/%d)", n.id, oldCommit, N, count, len(n.peers))
			}
			n.triggerApply() // Use signal instead of spawn go n.applyEntries
		} else {
			break // Can't commit higher indices yet
		}
	}
}

// Modified updateCommitIndex to notify waiters
func (n *Node) updateCommitIndexWithBatching() {
	// Caller must hold n.mu

	lastLogIndex := n.getLastLogIndex()
	oldCommit := n.commitIndex
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
			n.commitIndex = N

			// Sample logging
			if shouldLog(N, 10) {
				log.Printf("[%s] Advancing commitIndex from %d to %d (majority confirmed: %d/%d)", n.id, oldCommit, N, count, len(n.peers))
			}
		} else {
			break // Can't commit higher indices yet
		}
	}

	// Notify all waiters in on shot
	if n.commitIndex > oldCommit {
		go n.NotifyBatchCommitWaiters(oldCommit, n.commitIndex)
		n.triggerApply() // Use signal instead of spawn go n.applyEntries
	}
}

// applyEntries applies committed entries to state machine
func (n *Node) applyEntries() {
	// Only one goroutine can apply at a time
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	for {
		// Phase 1: Get next entry to apply (locked)
		n.mu.Lock()

		// check if there's work to do
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

				// Entry not found and not in snapshot
				log.Printf("[%s] CRITICAL: Entry at index %d not found (lastApplied=%d, commitIndex=%d, log.len=%d)", n.id, nextIndex, n.lastApplied, n.commitIndex, len(n.log))

				// Debug: Print current log
				log.Printf("[%s] Current log entries:", n.id)
				for i, e := range n.log {
					log.Printf("[%s]   log[%d]: index=%d, term=%d", n.id, i, e.Index, e.Term)
				}
				n.mu.Unlock()
				return // Stop applying
			}
		}

		// Copy command data to avoid holding lock during Apply
		entryIndex := entry.Index
		entryCommand := make([]byte, len(entry.Command))
		copy(entryCommand, entry.Command)

		// Phase 2: Apply without holding lock (kvStore might be slow)
		n.mu.Unlock()

		err := n.kvStore.Apply(entryCommand)
		if err != nil {
			log.Printf("[%s] Failed to apply entry %d: %v", n.id, entryIndex, err)
			break // stop on error
		}

		// Update lastApplied
		n.mu.Lock()
		n.lastApplied = entryIndex
		n.notifyApplyWaiters(entryIndex)
		// Log every 10th entry
		if shouldLog(entryIndex, 10) {
			log.Printf("[%s] Applied entry index=%d (lastApplied=%d, commitIndex=%d)", n.id, entryIndex, n.lastApplied, n.commitIndex)
		}
		n.mu.Unlock()
	}
}

// same with replicationCoordinator, applyCoordinator also runs as single goroutine
func (n *Node) applyCoordinator(ctx context.Context) {
	defer close(n.applyDone)

	log.Printf("[%s] Apply coordinator started", n.id)

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applySignal:
			// Signal received, apply entries
			n.applyEntries()
		case <-n.shutdownCh:
			log.Printf("[%s] Apply coordinator stopping", n.id)
			return
		}
	}
}

// triggerApply an helper to trigger apply entries (non-blocking)
func (n *Node) triggerApply() {
	select {
	case n.applySignal <- struct{}{}:
		// Signal sent
	default:
		// Already signaled, perfect!
	}
}
