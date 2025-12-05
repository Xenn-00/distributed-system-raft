package raft

import (
	"context"
	"sync"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/kv"
	"github.com/Xenn-00/distributed-kv-store/storage"
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

	// Channels
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	shutdownCh     chan struct{}

	// gRPC clients
	clients   map[string]pb.RaftClient
	clientsMu sync.RWMutex

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
	replicationQueue chan string   // peerID to replicate to
	replicationStop  chan struct{} // stop workers

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
