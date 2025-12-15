package kv

import (
	"fmt"
	"maps"
	"sync"

	"github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/commandpb"
	"google.golang.org/protobuf/proto"
)

// KVStore is the state machine
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string // simple in-memory key-value store
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Set: sets the value for a given key
func (k *KVStore) Set(key, value string) {
	// Write lock for exclusive access
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = value
}

// Get: retrieves the value for a given key
func (k *KVStore) Get(key string) (string, bool) {
	// Read lock for concurrent reads
	k.mu.RLock()
	defer k.mu.RUnlock()
	value, ok := k.data[key]
	return value, ok
}

// Delete: removes a key from the store
func (k *KVStore) Delete(key string) {
	// Write lock for exclusive access
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.data, key)
}

// GetAll: retrives all key-value pairs
func (k *KVStore) GetAll() map[string]string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	// Return copy
	result := make(map[string]string)
	maps.Copy(result, k.data)

	return result
}

// Apply: applies a command to the KV store
func (k *KVStore) Apply(cmdBytes []byte) error {
	cmd := &commandpb.Command{}
	if err := proto.Unmarshal(cmdBytes, cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	switch cmd.Op {
	case commandpb.Command_SET:
		k.Set(cmd.Key, cmd.Value)
	case commandpb.Command_DELETE:
		k.Delete(cmd.Key)
	case commandpb.Command_GET:
		// GET doesn't modify state, so no action needed
	default:
		return nil
	}

	return nil
}

// RestoreFromSnapshot: restores KV state from snapshot data
func (k *KVStore) RestoreFromSnapshot(data map[string]string) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Clear existing data
	k.data = make(map[string]string, len(data))

	// Copy snapshot data
	maps.Copy(k.data, data)
}
