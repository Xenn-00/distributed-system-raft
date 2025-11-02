package server

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/Xenn-00/distributed-kv-store/raft"
	"google.golang.org/grpc"
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

func (s *RaftServer) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, s)

	log.Printf("gRPC server listening on %s", address)
	return grpcServer.Serve(lis)
}
