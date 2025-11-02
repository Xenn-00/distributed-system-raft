package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Xenn-00/distributed-kv-store/raft"
	"github.com/Xenn-00/distributed-kv-store/server"
)

func main() {
	// Application entry point
	nodeID := flag.String("id", "node1", "Node ID")
	address := flag.String("addr", "localhost:5001", "Node address")
	flag.Parse()

	// Harcoded 3-node cluster for demonstration
	peers := map[string]string{
		"node1": "localhost:5001",
		"node2": "localhost:5002",
		"node3": "localhost:5003",
	}

	log.Printf("Starting Raft node: %s at %s", *nodeID, *address)

	// Create Raft node
	node := raft.NewNode(*nodeID, peers)
	node.Start()

	// Start gRPC server
	raftServer := server.NewRaftServer(node)
	go func() {
		if err := raftServer.Start(*address); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	node.Shutdown()
}
