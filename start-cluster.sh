#!/bin/bash

echo "Killing existing processes..."
pkill -f "raftnode" 2>/dev/null || true
sleep 1

echo "Starting node1..."
go run cmd/raftnode/main.go -id=node1 -addr=localhost:5001 > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "Node1 started (PID: $NODE1_PID)"

sleep 5

echo "Starting node2..."
go run cmd/raftnode/main.go -id=node2 -addr=localhost:5002 > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "Node2 started (PID: $NODE2_PID)"

sleep 5

echo "Starting node3..."
go run cmd/raftnode/main.go -id=node3 -addr=localhost:5003 > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "Node3 started (PID: $NODE3_PID)"

echo ""
echo "All nodes started!"
echo "Node1: PID $NODE1_PID (localhost:5001)"
echo "Node2: PID $NODE2_PID (localhost:5002)"
echo "Node3: PID $NODE3_PID (localhost:5003)"
echo ""
echo "Tailing logs... (Ctrl+C to stop)"
echo ""

# Create logs directory
mkdir -p logs

# Tail all logs
tail -f logs/node1.log logs/node2.log logs/node3.log