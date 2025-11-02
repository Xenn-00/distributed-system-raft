#!/bin/bash
protoc --go_out=. --go-grpc_out=. proto/raft.proto
echo "✓ Proto files generated!"