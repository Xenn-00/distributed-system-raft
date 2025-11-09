package storage

import (
	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
)

// Storage interface for persisting Raft state
type Storage interface {
	// Raft persistence state
	SaveTerm(term uint64) error
	LoadTerm() (uint64, error)

	SaveVote(votedFor string) error
	LoadVote() (string, error)

	// Log operations
	AppendLog(entry *pb.LogEntry) error
	AppendLogs(entries []*pb.LogEntry) error
	GetLog(index uint64) (*pb.LogEntry, error)
	GetAllLogs() ([]*pb.LogEntry, error)
	GetLogsFrom(startIndex uint64) ([]*pb.LogEntry, error)
	TruncateLogFrom(index uint64) error
	GetLastLogIndex() (uint64, error)

	// Snapshot
	SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error
	LoadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, err error)
	HasSnapshot() bool

	// KV state (for snapshot)
	SaveKVState(data map[string]string) error
	LoadKVState() (map[string]string, error)

	// Lifecycle
	Close() error
}
