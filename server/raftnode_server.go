package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type RaftServer struct {
	pb.UnimplementedRaftServer
	node *raft.Node
}

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

func (s *RaftServer) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		// Limit concurrent streams per connection
		grpc.MaxConcurrentStreams(50),

		// Limit max message size received
		grpc.MaxRecvMsgSize(16*1024*1024), // 16MB

		// Limit max message size send
		grpc.MaxSendMsgSize(16*1024*1024),

		// Set keep alive params
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Minute,
			MaxConnectionAge:  30 * time.Minute,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
	)
	pb.RegisterRaftServer(grpcServer, s)

	log.Printf("gRPC server listening on %s", address)
	return grpcServer.Serve(lis)
}
