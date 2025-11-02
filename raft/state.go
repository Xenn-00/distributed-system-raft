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

// Timeouts
const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 1000 * time.Millisecond
	ElectionTimeoutMax = 3000 * time.Millisecond
)
