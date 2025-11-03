package kv

import (
	"encoding/json"
	"maps"
	"sync"
)

// Command represents a KV operation
type Command struct {
	Op    string `json:"op"` // "SET", "GET", "DELETE"
	Key   string `json:"key"`
	Value string `json:"value"`
}

// KVStore is the state machine
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string // mimic-ing a simple in-memory key-value store
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
	var cmd Command
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		return err
	}
	switch cmd.Op {
	case "SET":
		k.Set(cmd.Key, cmd.Value)
	case "DELETE":
		k.Delete(cmd.Key)
		// GET doesn't modify state, so no action needed
	default:
		return nil
	}

	return nil
}
