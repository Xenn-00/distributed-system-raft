package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/kvpb"
	"github.com/Xenn-00/distributed-kv-store/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type KVServer struct {
	pb.UnimplementedKVServer
	node       *raft.Node
	grpcServer *grpc.Server
}

func NewKVServer(node *raft.Node) *KVServer {
	return &KVServer{node: node}
}

// Get handles read requests
func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	resp := &pb.GetResponse{
		Found: false,
	}

	// Check if we're the leader
	isLeader, leaderID, leaderAddress := s.node.GetLeaderInfo()
	resp.IsLeader = isLeader
	resp.LeaderId = leaderID
	resp.LeaderAddress = leaderAddress

	// Linearizable read: must be leader
	if req.Linearizable && !isLeader {
		log.Printf("[KV] Redirecting linearizable GET to leader: %s", leaderAddress)
		return resp, nil
	}

	// For linearizable reads on leader: wait for commitIndex to advance
	// This ensures we don't read stale data
	if req.Linearizable && isLeader {
		if err := s.node.WaitForCommit(ctx); err != nil {
			return nil, fmt.Errorf("failed to ensure consistency: %v", err)
		}
	}

	// Read from local KV store
	value, found := s.node.GetValue(req.Key)
	resp.Value = value
	resp.Found = found
	resp.RaftIndex = s.node.GetCommitIndex()

	return resp, nil
}

// Set handles write requests (must be on leader)
func (s *KVServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	resp := &pb.SetResponse{
		Success: false,
	}

	// Check if we're the leader
	isLeader, leaderID, leaderAddress := s.node.GetLeaderInfo()
	resp.IsLeader = isLeader
	resp.LeaderId = leaderID
	resp.LeaderAddress = leaderAddress

	if !isLeader {
		log.Printf("[KV] Redirecting SET to leader: %s", leaderAddress)
		return resp, nil
	}

	// Create command
	cmd := map[string]any{
		"op":    "SET",
		"key":   req.Key,
		"value": req.Value,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %v", err)
	}

	// Propose to Raft (blocks until committed or timeout)
	index, err := s.node.ProposalAsync(ctx, cmdBytes)
	if err != nil {
		// Check if we're still leader
		if !s.node.IsLeader() {
			resp.IsLeader = false
			_, resp.LeaderId, resp.LeaderAddress = s.node.GetLeaderInfo()
			resp.Success = false
			return resp, nil
		}
		return nil, fmt.Errorf("failed to propose: %v", err)
	}
	resp.Success = true
	resp.RaftIndex = index

	return resp, nil
}

// Delete handles delete requests
func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	resp := &pb.DeleteResponse{
		Success: false,
	}

	isLeader, leaderID, leaderAddress := s.node.GetLeaderInfo()
	resp.IsLeader = isLeader
	resp.LeaderId = leaderID
	resp.LeaderAddress = leaderAddress

	if !isLeader {
		log.Printf("[KV] Redirecting DELETE to leader: %s", leaderAddress)
		return resp, nil
	}

	cmd := map[string]any{
		"op":  "DELETE",
		"key": req.Key,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %v", err)
	}

	index, err := s.node.ProposalAsync(ctx, cmdBytes)
	if err != nil {
		if !s.node.IsLeader() {
			resp.IsLeader = false
			_, resp.LeaderId, resp.LeaderAddress = s.node.GetLeaderInfo()
			resp.Success = false
			return resp, nil
		}

		return nil, err
	}

	resp.Success = true
	resp.RaftIndex = index
	return resp, nil
}

// List handles list requests (can be stale)
func (s *KVServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	resp := &pb.ListResponse{}

	isLeader, leaderID, leaderAddress := s.node.GetLeaderInfo()
	resp.IsLeader = isLeader
	resp.LeaderId = leaderID
	resp.LeaderAddress = leaderAddress

	// List can be stale, read from local store
	entries := s.node.ListEntries(req.Prefix, int(req.Limit))
	resp.Entries = entries

	return resp, nil
}

func (s *KVServer) Start(address string) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		// Limit concurrent streams per connection
		grpc.MaxConcurrentStreams(100),

		// Limit max message size received
		grpc.MaxRecvMsgSize(4*1024*1024), // 4MB

		// Limit max message size send
		grpc.MaxSendMsgSize(4*1024*1024),

		// Keepalive params
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),

		// Set keep alive params
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			MaxConnectionAge:  10 * time.Minute,
			Time:              1 * time.Minute,
			Timeout:           20 * time.Second,
		}),

		// Connection timeout
		grpc.ConnectionTimeout(10*time.Second),
	)
	pb.RegisterKVServer(grpcServer, s)

	s.grpcServer = grpcServer // Save reference

	log.Printf("gRPC server listening on %s", address)
	return grpcServer, lis, nil
}

// Graceful shutdown method
func (s *KVServer) Shutdown() {
	if s.grpcServer == nil {
		return
	}

	log.Printf("[KVServer] Shutting down gracefully...")

	// Graceful stop (waits for ongoing RPCs)
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	// Timeout if takes too long
	select {
	case <-done:
		log.Printf("[KVServer] Shutdown complete")
	case <-time.After(5 * time.Second):
		log.Printf("[KVServer] Forcing shutdown after timeout")
		s.grpcServer.Stop() // Force stop
	}
}
