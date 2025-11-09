package storage

import (
	"encoding/json"
	"fmt"
	"log"

	pb "github.com/Xenn-00/distributed-kv-store/github.com/Xenn-00/distributed-kv-store/proto/raftpb"
	"github.com/dgraph-io/badger/v4"
)

// Key prefixes, all in bytes because all KV db like BadgerDB, LevelDB, RocksDB etc only store bytes
var (
	keyCurrentTerm   = []byte("term")
	keyVotedFor      = []byte("vote")
	keyLogPrefix     = []byte("log:")
	keySnapshotIndex = []byte("snap:index")
	keySnapshotTerm  = []byte("snap:term")
	keySnapshotData  = []byte("snap:data")
	keyKVState       = []byte("kv:state")
)

type BadgerStorage struct {
	db *badger.DB
}

func NewBadgerStroage(dataDir string) (*BadgerStorage, error) {
	opts := badger.DefaultOptions(dataDir)
	opts.Logger = nil // Disable badger's verbose logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %v", err)
	}

	log.Printf("Opened BadgerDB at %s", dataDir)
	return &BadgerStorage{db: db}, nil
}

// ======== Term ========
func (s *BadgerStorage) SaveTerm(term uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := uint64ToBytes(term)
		return txn.Set(keyCurrentTerm, val)
	})
}

func (s *BadgerStorage) LoadTerm() (uint64, error) {
	var term uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyCurrentTerm)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				term = 0
				return nil
			}
		}

		return item.Value(func(val []byte) error {
			term = bytesToUint64(val)
			return nil
		})
	})

	return term, err
}

// ======== Vote ========
func (s *BadgerStorage) SaveVote(votedFor string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyVotedFor, []byte(votedFor))
	})
}

func (s *BadgerStorage) LoadVote() (string, error) {
	var votedFor string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyVotedFor)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				votedFor = ""
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			votedFor = string(val)
			return nil
		})
	})

	return votedFor, err
}

// ======== Log ========
func (s *BadgerStorage) AppendLog(entry *pb.LogEntry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		key := logKey(entry.Index)
		val, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		return txn.Set(key, val)
	})
}

func (s *BadgerStorage) AppendLogs(entries []*pb.LogEntry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			key := logKey(entry.Index)
			val, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStorage) GetLog(index uint64) (*pb.LogEntry, error) {
	var entry *pb.LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(index))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			entry = &pb.LogEntry{}
			return json.Unmarshal(val, entry)
		})
	})
	return entry, err
}

func (s *BadgerStorage) GetAllLogs() ([]*pb.LogEntry, error) {
	var logs []*pb.LogEntry

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = keyLogPrefix

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				entry := &pb.LogEntry{}
				if err := json.Unmarshal(val, entry); err != nil {
					return err
				}
				logs = append(logs, entry)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return logs, err
}

func (s *BadgerStorage) GetLogsFrom(startIndex uint64) ([]*pb.LogEntry, error) {
	var logs []*pb.LogEntry

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = keyLogPrefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to startIndex
		seekKey := logKey(startIndex)
		for it.Seek(seekKey); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				entry := &pb.LogEntry{}
				if err := json.Unmarshal(val, entry); err != nil {
					return err
				}
				logs = append(logs, entry)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	return logs, err
}

func (s *BadgerStorage) TruncateLogFrom(index uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = keyLogPrefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Find all keys >= index and delete them
		seekKey := logKey(index)
		var keysToDelete [][]byte

		for it.Seek(seekKey); it.Valid(); it.Next() {
			item := it.Item()
			keysToDelete = append(keysToDelete, item.KeyCopy(nil))
		}

		// Delete keys
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStorage) GetLastLogIndex() (uint64, error) {
	var lastIndex uint64

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = keyLogPrefix
		opts.Reverse = true

		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if !it.Valid() {
			lastIndex = 0
			return nil
		}

		item := it.Item()
		return item.Value(func(val []byte) error {
			entry := &pb.LogEntry{}
			if err := json.Unmarshal(val, entry); err != nil {
				return err
			}
			lastIndex = entry.Index
			return nil
		})
	})

	return lastIndex, err
}

// ======== Snapshot ========

func (s *BadgerStorage) SaveSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(keySnapshotIndex, uint64ToBytes(lastIncludedIndex)); err != nil {
			return err
		}
		if err := txn.Set(keySnapshotTerm, uint64ToBytes(lastIncludedTerm)); err != nil {
			return err
		}
		if err := txn.Set(keySnapshotData, data); err != nil {
			return err
		}

		return nil
	})
}

func (s *BadgerStorage) LoadSnapshot() (lastIncludedIndex, lastIncludedTerm uint64, data []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		// Load index
		item, err := txn.Get(keySnapshotIndex)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		err = item.Value(func(val []byte) error {
			lastIncludedIndex = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return err
		}

		// Load term
		item, err = txn.Get(keySnapshotTerm)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			lastIncludedTerm = bytesToUint64(val)
			return nil
		})
		if err != nil {
			return err
		}

		// Load data
		item, err = txn.Get(keySnapshotData)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			data = append(data, val...)
			return nil
		})
	})
	return
}

func (s *BadgerStorage) HasSnapshot() bool {
	var hasSnap bool
	s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(keySnapshotIndex)
		hasSnap = (err == nil)
		return nil
	})

	return hasSnap
}

// ======== KV State ========

func (s *BadgerStorage) SaveKVState(data map[string]string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val, err := json.Marshal(data)
		if err != nil {
			return err
		}
		return txn.Set(keyKVState, val)
	})
}

func (s *BadgerStorage) LoadKVStore() (map[string]string, error) {
	var data map[string]string

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyKVState)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				data = make(map[string]string)
				return nil
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &data)
		})
	})

	return data, err
}

// ======== Lifecycle ========

func (s *BadgerStorage) Close() error {
	log.Println("Closing BadgerDB")
	return s.db.Close()
}
