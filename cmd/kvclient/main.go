package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		addr   = flag.String("addr", "localhost:6001", "KV server address")
		op     = flag.String("op", "get", "Operation: get, set, delete, list")
		key    = flag.String("key", "", "Key")
		value  = flag.String("value", "", "Value (for set)")
		linear = flag.Bool("linear", false, "Linearizable read (default: flase)")
	)
	flag.Parse()

	fmt.Printf("...Connecting to KV server at %s...\n", *addr)

	// Connect to KV server
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *op {
	case "get":
		get(ctx, client, *key, *linear)
	case "set":
		set(ctx, client, *key, *value)
	case "del":
		del(ctx, client, *key)
	case "list":
		list(ctx, client)
	default:
		log.Fatalf("❌ Unknown operation: %s", *op)
	}
}

func get(ctx context.Context, client pb.KVClient, key string, linear bool) {
	if key == "" {
		log.Fatal("❌ Key is required for GET")
	}

	fmt.Printf("📖 GET %s (linearizable=%v)\n", key, linear)

	resp, err := client.Get(ctx, &pb.GetRequest{
		Key:          key,
		Linearizable: linear,
	})
	if err != nil {
		log.Fatalf("❌ GET failed: %v", err)
	}

	// Handle redirect
	if !resp.IsLeader && resp.LeaderAddress != "" {
		fmt.Printf("↪️  Not leader! Redirecting to %s (%s)...\n", resp.LeaderId, resp.LeaderAddress)

		// Auto-retry on leader
		conn, err := grpc.NewClient(resp.LeaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("❌ Failed to connect to leader: %v", err)
		}
		defer conn.Close()

		leaderClient := pb.NewKVClient(conn)
		resp, err = leaderClient.Get(ctx, &pb.GetRequest{
			Key:          key,
			Linearizable: linear,
		})
		if err != nil {
			log.Fatalf("❌ GET on leader failed: %v", err)
		}
	}

	// Display result
	if resp.Found {
		fmt.Printf("✅ Value: %s\n", resp.Value)
		fmt.Printf("   (committed at index %d)\n", resp.RaftIndex)
	} else {
		fmt.Printf("❌ Key not found\n")
	}
}

func set(ctx context.Context, client pb.KVClient, key, value string) {
	if key == "" || value == "" {
		log.Fatal("❌ Key and value are required for SET")
	}

	fmt.Printf("✍️ SET %s = %s \n", key, value)
	resp, err := client.Set(ctx, &pb.SetRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		log.Fatalf("❌ SET failed: %v", err)
	}

	// Handle redirect
	if !resp.IsLeader && resp.LeaderAddress != "" {
		fmt.Printf("↪️  Not leader! Redirecting to %s (%s)...\n", resp.LeaderId, resp.LeaderAddress)

		conn, err := grpc.NewClient(resp.LeaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("❌ Failed to connect to leader: %v", err)
		}
		defer conn.Close()

		leaderClient := pb.NewKVClient(conn)
		resp, err = leaderClient.Set(ctx, &pb.SetRequest{
			Key:   key,
			Value: value,
		})
		if err != nil {
			log.Fatalf("❌ SET on leader failed: %v", err)
		}
	}

	// Display result
	if resp.Success {
		fmt.Printf("✅ SET successful!\n")
		fmt.Printf("   (committed at index %d)\n", resp.RaftIndex)
	} else {
		fmt.Printf("❌ SET failed\n")
	}
}

func del(ctx context.Context, client pb.KVClient, key string) {
	if key == "" {
		log.Fatal("❌ Key is required for DELETE")
	}

	fmt.Printf("🗑️  DELETE %s\n", key)

	resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		log.Fatalf("❌ DELETE failed: %v", err)
	}

	// Handle redirect
	if !resp.IsLeader && resp.LeaderAddress != "" {
		fmt.Printf("↪️  Not leader! Redirecting to %s (%s)...\n", resp.LeaderId, resp.LeaderAddress)

		conn, err := grpc.NewClient(resp.LeaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("❌ Failed to connect to leader: %v", err)
		}
		defer conn.Close()

		leaderClient := pb.NewKVClient(conn)
		resp, err = leaderClient.Delete(ctx, &pb.DeleteRequest{Key: key})
		if err != nil {
			log.Fatalf("❌ DELETE on leader failed: %v", err)
		}
	}

	if resp.Success {
		fmt.Printf("✅ DELETE successful!\n")
		fmt.Printf("   (committed at index %d)\n", resp.RaftIndex)
	} else {
		fmt.Printf("❌ DELETE failed\n")
	}
}

func list(ctx context.Context, client pb.KVClient) {
	fmt.Printf("📋 LIST all entries\n")

	resp, err := client.List(ctx, &pb.ListRequest{})
	if err != nil {
		log.Fatalf("❌ LIST failed: %v", err)
	}

	fmt.Printf("\n📊 Total entries: %d\n", len(resp.Entries))

	if len(resp.Entries) == 0 {
		fmt.Println("   (empty)")
	} else {
		fmt.Println()
		for k, v := range resp.Entries {
			fmt.Printf("   %s = %s\n", k, v)
		}
	}

	if !resp.IsLeader {
		fmt.Printf("\n⚠️  Note: Read from follower (data might be slightly stale)\n")
		fmt.Printf("   Current leader: %s (%s)\n", resp.LeaderId, resp.LeaderAddress)
	}
}
