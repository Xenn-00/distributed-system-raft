package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Xenn-00/distributed-kv-store/raft"
	"github.com/Xenn-00/distributed-kv-store/server"
	"google.golang.org/grpc"
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

	myAddr, ok := peers[*nodeID]
	if !ok {
		log.Fatalf("Unknown node ID: %s", *nodeID)
	}

	host, portStr, err := net.SplitHostPort(myAddr)
	if err != nil {
		log.Fatalf("Invalid address format: %v", err)
	}

	portInt, _ := strconv.Atoi(portStr)
	pprofPort := portInt + 2000

	pprofAddr := fmt.Sprintf("%s:%d", host, pprofPort)

	// Start pprof server
	go func() {
		log.Printf("[PPROF] %s is running at http://%s/debug/pprof", *nodeID, pprofAddr)
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			log.Printf("Failed to start pprof: %v", err)
		}
	}()

	// Create Raft node
	node, err := raft.NewNode(*nodeID, peers, *dataDir)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	node.Start()

	// Start gRPC server
	raftServer := server.NewRaftServer(node)
	raftGrpcServer, raftListener, err := raftServer.Start(*address)
	if err != nil {
		log.Fatalf("Failed to start Raft server: %v", err)
	}

	// Serve in goroutine
	go func() {
		log.Printf("Raft gRPC server starting on %s", *address)
		if err := raftGrpcServer.Serve(raftListener); err != nil {
			log.Printf("Raft server stopped: %v", err)
		}
	}()

	// Monitor goroutines
	go monitorGoroutines(*nodeID)

	// Start KV server (client-facing) if address provided
	var kvServer *server.KVServer
	var kvGrpcServer *grpc.Server
	var kvListener net.Listener

	if *kvAddr != "" {
		kvServer = server.NewKVServer(node)
		kvGrpcServer, kvListener, err = kvServer.Start(*kvAddr)
		if err != nil {
			log.Fatalf("Failed to start KV server: %v", err)
		}

		go func() {
			log.Printf("Starting KV API server at %s", *kvAddr)
			if err := kvGrpcServer.Serve(kvListener); err != nil {
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

	// Graceful shutdown in proper order
	log.Printf("[%s] Stopping gRPC servers...", *nodeID)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop server gracefully
	stopped := make(chan struct{})
	go func() {
		// Stop Raft server
		if raftServer != nil {
			raftServer.Shutdown()
		}

		if kvServer != nil {
			kvServer.Shutdown()
		}

		close(stopped)
	}()

	select {
	case <-stopped:
		log.Printf("[%s] gRPC servers stopped gracefully", *nodeID)
	case <-shutdownCtx.Done():
		log.Printf("[%s] WARNING: Server shutdown timeout, forcing stop", *nodeID)
		if raftGrpcServer != nil {
			raftGrpcServer.Stop() // force stop
		}
		if kvGrpcServer != nil {
			kvGrpcServer.Stop() // force stop
		}
	}

	// Stop Raftnode
	log.Printf("[%s] Stopping Raft node...", *nodeID)
	node.Shutdown()

	// Final goroutine check
	time.Sleep(500 * time.Millisecond)
	finalCount := runtime.NumGoroutine()
	log.Printf("[%s] Shutdown complete. Final goroutine count: %d", *nodeID, finalCount)

	if finalCount > 10 {
		log.Printf("[%s] WARNING: %d goroutines still running (expected < 10)", *nodeID, finalCount)

		// Dump goroutines for debugging
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		log.Printf("[%s] Goroutine dump:\n%s", *nodeID, buf[:n])
	}
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
	fmt.Println("  debug              - Debug runtime stack")
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
			index, err := node.ProposalAsync(ctx, cmdBytes)
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
			index, err := node.ProposalAsync(ctx, cmdBytes)
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

			// fmt.Printf("Replication status: %v\n", replicationStatus)

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
		case "debug":
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			stacks := string(buf[:n])

			counts := make(map[string]int)

			// Count patterns
			patterns := map[string]string{
				"grpc_server":   "google.golang.org/grpc/internal/transport.(*http2Server)",
				"grpc_client":   "google.golang.org/grpc/internal/transport.(*http2Client)",
				"badger":        "github.com/dgraph-io/badger",
				"raft_pipeline": "github.com/Xenn-00/distributed-kv-store/raft.(*PeerReplicator)",
				"raft_other":    "github.com/Xenn-00/distributed-kv-store/raft",
			}

			for name, pattern := range patterns {
				counts[name] = strings.Count(stacks, pattern)
			}

			fmt.Printf("=== Goroutine Analysis ===\n")
			fmt.Printf("Total: %d\n", runtime.NumGoroutine())
			for name, count := range counts {
				fmt.Printf("  %s: ~%d\n", name, count)
			}

		default:
			fmt.Println("Unknown command.")
		}
	}
}

// Monitor goroutines periodically
func monitorGoroutines(nodeID string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	lastCount := 0

	for range ticker.C {
		count := runtime.NumGoroutine()
		delta := count - lastCount

		if delta > 0 {
			log.Printf("[%s] Goroutines: %d (+%d)", nodeID, count, delta)
		} else if delta < 0 {
			log.Printf("[%s] Goroutines: %d (%d)", nodeID, count, delta)
		} else {
			log.Printf("[%s] Goroutines: %d (stable)", nodeID, count)
		}

		if count > 500 {
			log.Printf("[%s] ⚠️  WARNING: High goroutine count (%d)!", nodeID, count)
		}

		if count > 5000 {
			log.Printf("[%s] 🔥 CRITICAL: Goroutine leak detected (%d)!", nodeID, count)
			// Auto-dump for debugging
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)

			filename := fmt.Sprintf("goroutine-leak-%s-%d.txt", nodeID, time.Now().Unix())
			os.WriteFile(filename, buf[:n], 0644)
			log.Printf("[%s] Goroutine dump saved to %s", nodeID, filename)
		}

		lastCount = count
	}
}
