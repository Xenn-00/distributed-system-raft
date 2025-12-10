package raft

import "time"

// NodeState represents Raft node states.
type NodeState int

const (
	Follower NodeState = iota // iota means 0
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Timeouts and Snapshots
const (
	HeartbeatInterval  = 150 * time.Millisecond
	ElectionTimeoutMin = 1000 * time.Millisecond
	ElectionTimeoutMax = 3000 * time.Millisecond
	// Snapshot ever N log entries
	SnapshotThreshold = 10 // Set low for testing, production could use 10000+
	// Time-based trigger
	SnapshotInterval = 3 * time.Minute
	// Minimum entries before time-based snapshot
	MinEntriesForSnapshot = 5 // Don't snapshot if <5 entries
)
