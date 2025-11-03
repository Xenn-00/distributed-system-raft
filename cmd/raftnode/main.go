package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Xenn-00/distributed-kv-store/kv"
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

	// Interactive CLI for testing
	go func() {
		time.Sleep(2 * time.Second) // Wait for cluster to stabilize

		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("\nRaft KV Store CLI")
		fmt.Println("Commands:")
		fmt.Println("  set <key> <value>  - Set a key-value pair")
		fmt.Println("  get <key>          - Get a value")
		fmt.Println("  del <key>          - Delete a key")
		fmt.Println("  list               - List all keys")
		fmt.Println("  status             - Show node status")
		fmt.Println()

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

				err := node.Propose(kv.Command{
					Op:    "SET",
					Key:   parts[1],
					Value: parts[2],
				})
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK (proposed, waiting for commit...)")
				}

			case "get":
				if len(parts) != 2 {
					fmt.Println("Usage: get <key>")
					continue
				}

				kvStore := node.GetKVStore()
				val, ok := kvStore.Get(parts[1])
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

				err := node.Propose(kv.Command{
					Op:  "DELETE",
					Key: parts[1],
				})
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK (proposed)")
				}

			case "list":
				kvStore := node.GetKVStore()
				data := kvStore.GetAll()
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

				fmt.Printf("Node: %s\n", *nodeID)
				fmt.Printf("Leader: %v\n", isLeader)

				if !isLeader {
					if leaderID != "" {
						fmt.Printf("Current Leader: %s (%s)\n", leaderID, leaderAddr)
					} else {
						fmt.Println("Current Leader: Unknown (election in progress)")
					}
				}

			default:
				fmt.Println("Unknown command. Type 'help' for usage.")
			}
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	node.Shutdown()
}
