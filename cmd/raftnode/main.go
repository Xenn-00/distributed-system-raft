package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Xenn-00/distributed-kv-store/raft"
	"github.com/Xenn-00/distributed-kv-store/server"
)

func main() {
	// Application entry point
	nodeID := flag.String("id", "node1", "Node ID")
	address := flag.String("addr", "localhost:5001", "Node address")
	kvAddr := flag.String("kv-addr", "", "KV API address")
	dataDir := flag.String("data", "./data", "Data directory")
	flag.Parse()

	// Harcoded 3-node cluster for demonstration
	peers := map[string]string{
		"node1": "localhost:5001",
		"node2": "localhost:5002",
		"node3": "localhost:5003",
	}

	log.Printf("Starting Raft node: %s at %s (data: %s)", *nodeID, *address, *dataDir)

	// Create Raft node
	node, err := raft.NewNode(*nodeID, peers, *dataDir)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	node.Start()

	// Start gRPC server
	raftServer := server.NewRaftServer(node)
	go func() {
		if err := raftServer.Start(*address); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Start KV server (client-facing) if address provided
	if *kvAddr != "" {
		kvServer := server.NewKVServer(node)
		go func() {
			log.Printf("Starting KV API server at %s", *kvAddr)
			if err := kvServer.Start(*kvAddr); err != nil {
				log.Fatalf("Failed to start KV server: %v", err)
			}
		}()
	}

	// Interactive CLI for testing
	go startCLI(node, *nodeID)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	node.Shutdown()
}

func startCLI(node *raft.Node, nodeID string) {
	time.Sleep(2 * time.Second) // Wait for cluster to stabilize

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("\n=== Raft KV Store CLI (Direct Node Access) ===")
	fmt.Println("Commands:")
	fmt.Println("  set <key> <value>  - Set a key-value pair")
	fmt.Println("  get <key>          - Get a value")
	fmt.Println("  del <key>          - Delete a key")
	fmt.Println("  list               - List all keys")
	fmt.Println("  status             - Show node status")
	fmt.Println("\nNote: This CLI directly accesses Raft layer (for testing only)")
	fmt.Println("      In production, use KVClient which talks to KVServer")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		switch cmd {
		case "set":
			if len(parts) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}

			if !node.IsLeader() {
				leaderID := node.GetLeaderID()
				leaderAddr := node.GetLeaderAddress()

				if leaderID != "" {
					fmt.Printf("Error: Not leader. Current leader: %s (%s)\n", leaderID, leaderAddr)
				} else {
					fmt.Println("Error: Not leader. No leader elected yet, try again later.")
				}
				continue
			}

			// Create command
			cmdMap := map[string]any{
				"op":    "SET",
				"key":   parts[1],
				"value": parts[2],
			}
			cmdBytes, err := json.Marshal(cmdMap)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			// Propose with context (5s timeout)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			index, err := node.Propose(ctx, cmdBytes)
			cancel()

			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("OK (committed at index %d)\n", index)
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}

			val, ok := node.GetValue(parts[1])
			if ok {
				fmt.Printf("%s\n", val)
			} else {
				fmt.Println("(nil)")
			}

		case "del":
			if len(parts) != 2 {
				fmt.Println("Usage: del <key>")
				continue
			}

			if !node.IsLeader() {
				leaderID := node.GetLeaderID()
				leaderAddr := node.GetLeaderAddress()

				if leaderID != "" {
					fmt.Printf("Error: Not leader. Current leader: %s (%s)\n", leaderID, leaderAddr)
				} else {
					fmt.Println("Error: Not leader. No leader elected yet.")
				}
				continue
			}

			// Create command
			cmdMap := map[string]any{
				"op":  "DELETE",
				"key": parts[1],
			}
			cmdBytes, err := json.Marshal(cmdMap)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			// Propose with context
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			index, err := node.Propose(ctx, cmdBytes)
			cancel()

			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("OK (committed at index %d)\n", index)
			}

		case "list":
			data := node.ListEntries("", 0)
			if len(data) == 0 {
				fmt.Println("(empty)")
			} else {
				for k, v := range data {
					fmt.Printf("%s = %s\n", k, v)
				}
			}

		case "status":
			isLeader := node.IsLeader()
			leaderID := node.GetLeaderID()
			leaderAddr := node.GetLeaderAddress()

			// Get snapshot info
			hasSnapshot := node.HasSnapshot()
			var snapInfo string
			if hasSnapshot {
				lastIdx, lastTerm := node.GetSnapshotInfo()
				snapInfo = fmt.Sprintf("Yes (lastIndex=%d, lastTerm=%d)", lastIdx, lastTerm)
			} else {
				snapInfo = "No"
			}

			logSize := node.GetLogSize()
			commitIdx := node.GetCommitIndex()

			fmt.Printf("Node: %s\n", nodeID)
			fmt.Printf("Leader: %v\n", isLeader)

			if !isLeader {
				if leaderID != "" {
					fmt.Printf("Current Leader: %s (%s)\n", leaderID, leaderAddr)
				} else {
					fmt.Println("Current Leader: Unknown (election in progress)")
				}
			}

			fmt.Printf("Log Entries: %d\n", logSize)
			fmt.Printf("Commit Index: %d\n", commitIdx)
			fmt.Printf("Snapshot: %s\n", snapInfo)

		default:
			fmt.Println("Unknown command.")
		}
	}
}
