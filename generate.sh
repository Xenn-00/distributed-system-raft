#!/bin/bash
protoc --go_out=. --go-grpc_out=. proto/raft.proto &&
protoc --go_out=. --go-grpc_out=. proto/kv.proto
echo "✓ Proto files generated!"