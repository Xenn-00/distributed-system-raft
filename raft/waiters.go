package raft

import (
	"context"
	"fmt"
	"log"
	"time"
)

// registerCommitWaiter adds a waiter channel for a specific index
func (n *Node) registerCommitWaiter(index uint64) chan struct{} {

	waiterCh := make(chan struct{})

	// Load existing waiters or create new slice
	actual, _ := n.commitWaiters.LoadOrStore(index, &waitersEntry{})
	entry := actual.(*waitersEntry)

	entry.mu.Lock()
	entry.waiters = append(entry.waiters, waiterCh)
	entry.mu.Unlock()

	return waiterCh
}

// registerApplyWaiter has similar logic to registerCommitWaiter
func (n *Node) registerApplyWaiter(index uint64) chan struct{} {
	waiterCh := make(chan struct{})

	actual, _ := n.applyWaiters.LoadOrStore(index, &waitersEntry{})
	entry := actual.(*waitersEntry)

	entry.mu.Lock()
	entry.waiters = append(entry.waiters, waiterCh)
	entry.mu.Unlock()

	return waiterCh
}

// unregisterCommitWaiter removes a waiter (for cleanup on timeout/cancel)
func (n *Node) unregisterCommitWaiter(index uint64, waiterCh chan struct{}) {
	actual, ok := n.commitWaiters.Load(index)
	if !ok {
		return
	}
	entry := actual.(*waitersEntry)

	entry.mu.Lock()
	// filter out the channel
	new := entry.waiters[:0]
	for _, ch := range entry.waiters {
		if ch != waiterCh {
			new = append(new, ch)
		}
	}
	entry.waiters = new
	empty := len(entry.waiters) == 0
	entry.mu.Unlock()

	if empty {
		// safe to delete; other goroutines will re-create on demand
		n.commitWaiters.Delete(index)
	}
}

func (n *Node) unregisterApplyWaiter(index uint64, waiterCh chan struct{}) {
	actual, ok := n.applyWaiters.Load(index)
	if !ok {
		return
	}
	entry := actual.(*waitersEntry)

	entry.mu.Lock()
	new := entry.waiters[:0]
	for _, ch := range entry.waiters {
		if ch != waiterCh {
			new = append(new, ch)
		}
	}
	entry.waiters = new
	empty := len(entry.waiters) == 0
	entry.mu.Unlock()

	if empty {
		n.applyWaiters.Delete(index)
	}
}

// waitAndRespond waits for commit and sends response
func (n *Node) waitAndRespond(req *proposalRequest, index uint64, term uint64) {
	// Wait for commit
	err := n.waitForCommit(req.ctx, index)

	// Send response
	select {
	case req.respCh <- &proposalResponse{index: index, err: err}:
	case <-time.After(100 * time.Millisecond):
		log.Printf("[%s] Client abandoned proposal for index %d at term %d", n.id, index, term)
	}
}

// waitForCommit waits until the given index is committed (uses event-driven waiters)
func (n *Node) waitForCommit(ctx context.Context, index uint64) error {
	// Fast path: check immediately
	n.mu.Lock()
	if n.commitIndex >= index {
		n.mu.Unlock()
		return nil
	}
	if n.state != Leader {
		n.mu.Unlock()
		return fmt.Errorf("not leader")
	}
	n.mu.Unlock()

	// Register waiter for this specific index
	waiterCh := n.registerCommitWaiter(index)
	defer n.unregisterCommitWaiter(index, waiterCh)

	// Fallback ticker (safety net if notification missed)
	ticker := time.NewTicker(200 * time.Millisecond) // slow fallback only
	defer ticker.Stop()

	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			n.mu.Lock()
			current := n.commitIndex
			n.mu.Unlock()
			return fmt.Errorf("timeout waiting for commit (current=%d, target=%d)", current, index)
		case <-waiterCh:
			// Got notified, verify and return
			n.mu.Lock()
			committed := n.commitIndex >= index
			stillLeader := n.state == Leader
			n.mu.Unlock()

			if !stillLeader {
				return fmt.Errorf("no longer leader")
			}

			if committed {
				return nil // success
			}
			// edge case: notification but not committed yet, wait again
		case <-ticker.C:
			// Fallback: periodic check in case notification missed
			n.mu.Lock()
			committed := n.commitIndex >= index
			stillLeader := n.state == Leader
			n.mu.Unlock()

			if !stillLeader {
				return fmt.Errorf("no longer leader")
			}

			if committed {
				return nil // success
			}
		}
	}
}

// WaitForCommit ensures current commitIndex has been applied (for linearizable reads)
func (n *Node) WaitForCommit(ctx context.Context) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader")
	}

	// Record current commitIndex
	n.mu.Lock()
	readIndex := n.commitIndex
	n.mu.Unlock()

	// Wait for lastApplied to catch up
	return n.waitForApply(ctx, readIndex)
}

// waitForApply waits until lastApplied >= index, similar to waitForCommit
func (n *Node) waitForApply(ctx context.Context, index uint64) error {
	n.mu.Lock()
	if n.lastApplied >= index {
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()

	waiterCh := n.registerApplyWaiter(index)
	defer n.unregisterApplyWaiter(index, waiterCh)

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for apply")
		case <-waiterCh:
			n.mu.Lock()
			applied := n.lastApplied >= index
			n.mu.Unlock()
			if applied {
				return nil
			}
		case <-ticker.C:
			n.mu.Lock()
			applied := n.lastApplied >= index
			n.mu.Unlock()
			if applied {
				return nil
			}
		}
	}
}

// notifyCommitWaiters wakes up all goroutines waiting for this index
func (n *Node) NotifyCommitWaiters(index uint64) {
	actual, ok := n.commitWaiters.Load(index)
	if !ok {
		return
	}
	entry := actual.(*waitersEntry)

	// Lock, copy slice, and remove from map BEFORE closing channels to avoid races/duplicate close
	entry.mu.Lock()
	waitersCopy := make([]chan struct{}, len(entry.waiters))
	copy(waitersCopy, entry.waiters)
	entry.mu.Unlock()

	// Remove from map so re-registrations create a new entry if needed
	n.commitWaiters.Delete(index)

	for _, ch := range waitersCopy {
		close(ch)
	}
}

// notifyBatchCommitWaiters wakes up all waiters for committed indices
func (n *Node) NotifyBatchCommitWaiters(oldCommit, newCommit uint64) {
	// Notify all waiters for indices between oldCommit and newCommit
	for i := oldCommit + 1; i <= newCommit; i++ {
		n.NotifyCommitWaiters(i) // reuse existing method
	}
}

func (n *Node) notifyApplyWaiters(index uint64) {
	actual, ok := n.applyWaiters.Load(index)
	if !ok {
		return
	}
	entry := actual.(*waitersEntry)

	entry.mu.Lock()
	waitersCopy := make([]chan struct{}, len(entry.waiters))
	copy(waitersCopy, entry.waiters)
	entry.mu.Unlock()

	n.applyWaiters.Delete(index)

	for _, ch := range waitersCopy {
		close(ch)
	}
}

func (n *Node) NotifyBatchApplyWaiters(oldCommit, newCommit uint64) {
	for i := oldCommit + 1; i <= newCommit; i++ {
		n.notifyApplyWaiters(i)
	}
}
