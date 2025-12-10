package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type RaftServer struct {
	pb.UnimplementedRaftServer
	node       *raft.Node
	grpcServer *grpc.Server
}

var (
	activeConnections int32
	maxConnections    = int32(100) // Max 100 concurrent
)

func NewRaftServer(node *raft.Node) *RaftServer {
	return &RaftServer{node: node}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.node.RequestVote(ctx, req)
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.node.AppendEntries(ctx, req)
}

func (s *RaftServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	return s.node.InstallSnapshot(ctx, req)
}

func (s *RaftServer) Start(address string) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		// Limit concurrent streams per connection
		grpc.MaxConcurrentStreams(50),

		grpc.ChainUnaryInterceptor(
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
				current := atomic.LoadInt32(&activeConnections)
				if current > maxConnections {
					return nil, status.Error(codes.ResourceExhausted, "too many connections")
				}
				return handler(ctx, req)
			},
		),

		// Connection timeout
		grpc.ConnectionTimeout(10*time.Second),

		// Limit max message size received
		grpc.MaxRecvMsgSize(16*1024*1024), // 16MB

		// Limit max message size send
		grpc.MaxSendMsgSize(16*1024*1024),

		// Set keep alive params
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     30 * time.Second,
			MaxConnectionAge:      2 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second, // Force close after this
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
	)
	pb.RegisterRaftServer(grpcServer, s)

	s.grpcServer = grpcServer // Save reference

	log.Printf("gRPC server listening on %s", address)
	return grpcServer, lis, nil
}

// Graceful shutdown method
func (s *RaftServer) Shutdown() {
	if s.grpcServer == nil {
		return
	}

	log.Printf("[RaftServer] Shutting down gracefully...")

	// Graceful stop (waits for ongoing RPCs)
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	// Timeout if takes too long
	select {
	case <-done:
		log.Printf("[RaftServer] Shutdown complete")
	case <-time.After(5 * time.Second):
		log.Printf("[RaftServer] Forcing shutdown after timeout")
		s.grpcServer.Stop() // Force stop
	}
}
