package raft

import (
	"sync"

	"github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/commandpb"
)

var (
	commandPool = sync.Pool{
		New: func() any {
			return &commandpb.Command{}
		},
	}

	snapshotPool = sync.Pool{
		New: func() any {
			return &commandpb.SnapshotData{}
		},
	}
)

// GetCommand gets a Command message from pool
func GetCommand() *commandpb.Command {
	return commandPool.Get().(*commandpb.Command)
}

// PutCommand returns Command to pool after use
func PutCommand(cmd *commandpb.Command) {
	// Reset all fields before returning to pool
	cmd.Op = commandpb.Command_UNKNOWN
	cmd.Key = ""
	cmd.Value = ""
	cmd.Timestamp = 0
	cmd.ClientId = ""
	cmd.RequestId = 0

	commandPool.Put(cmd)
}

// GetSnapshotData gets a SnapshotData message from pool
func GetSnapshotData() *commandpb.SnapshotData {
	return snapshotPool.Get().(*commandpb.SnapshotData)
}

// PutSnapshotData returns SnapshotData to pool after use
func PutSnapshotData(snap *commandpb.SnapshotData) {
	// Clear map before returning
	snap.Data = nil
	snap.LastAppliedIndex = 0
	snap.LastAppliedTerm = 0
	snap.CreatedAt = 0

	snapshotPool.Put(snap)
}
