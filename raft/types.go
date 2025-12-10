package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/kv"
	"github.com/Xenn-00/distributed-kv-store/storage"
	"google.golang.org/grpc"
)

type Node struct {
	mu      sync.Mutex
	applyMu sync.Mutex

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

	// Heartbeat and election things
	electionTimer               *time.Timer
	consecutiveElectionFailures int // track election failures
	heartbeatTimer              *time.Ticker
	heartbeatCount              uint64 // track heartbeats
	heartbeatStop               chan struct{}
	heartbeatRunning            atomic.Bool // track if heartbeats is running
	lastHeartbeatAck            map[string]time.Time
	lastHeartbeatAckMu          sync.Mutex
	shutdownCh                  chan struct{}

	// gRPC clients
	clients     map[string]pb.RaftClient
	connections map[string]*grpc.ClientConn // Track connections
	clientsMu   sync.RWMutex

	// State machine (KV store)
	kvStore *kv.KVStore

	// Storage
	storage *storage.BadgerStorage

	// Snapshot tracking
	lastSnapshotTime  time.Time
	lastSnapshotIndex uint64

	// Event-driven notification system
	commitWaiters sync.Map
	applyWaiters  sync.Map

	// Proposal queue system
	proposalQueue chan *proposalRequest // Bounded queue
	ProposalSem   chan struct{}         // Semaphore for in-flight
	proposalStop  chan struct{}         // Stop proposal processor

	// Worker pool for bounded replcation
	replicationQueue     chan string   // peerID to replicate to
	replicationStop      chan struct{} // stop workers
	replicationSignal    chan struct{} // signal for replication needed
	replicationCoordDone chan struct{} // for clean shutdown

	// Apply signal
	applySignal chan struct{} // Signal when entries need applying
	applyDone   chan struct{} // for clean shutdown

	// Replication pipeline
	replicators   map[string]*PeerReplicator
	replicatorsMu sync.RWMutex

	// Rate limiting for failed replications
	replicationFailures map[string]int       // peerID -> consecutive failures
	lastFailureTime     map[string]time.Time // peerID -> last failure time
}

type waitersEntry struct {
	mu      sync.Mutex
	waiters []chan struct{}
}

type proposalRequest struct {
	command []byte
	respCh  chan *proposalResponse
	ctx     context.Context
}

type proposalResponse struct {
	index uint64
	err   error
}
