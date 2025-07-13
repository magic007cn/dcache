package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

const (
	// Internal namespace for dcache internal data
	InternalNamespace = "__dcache"
	// Raft WAL namespace
	RaftWALNamespace = "__dcache/raftwal"
)

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// BadgerStore implements the Raft FSM interface using BadgerDB
type BadgerStore struct {
	db   *badger.DB
	mu   sync.RWMutex
	log  *logrus.Logger
}

// NewBadgerStore creates a new BadgerStore instance
func NewBadgerStore(dataDir string, log *logrus.Logger) (*BadgerStore, error) {
	opts := badger.DefaultOptions(dataDir)
	opts.Logger = nil // Disable badger's internal logging
	opts.ValueLogFileSize = 1 << 20 // 1MB
	opts.NumVersionsToKeep = 1
	opts.MemTableSize = 64 << 20 // 64MB
	opts.ValueLogMaxEntries = 1000000
	
	// 配置为纯内存模式
	opts.InMemory = true // 启用内存模式
	opts.Dir = ""        // 空目录表示纯内存
	opts.ValueDir = ""   // 空值目录表示纯内存

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %v", err)
	}

	return &BadgerStore{
		db:  db,
		log: log,
	}, nil
}

// Apply applies a log entry to the FSM
func (s *BadgerStore) Apply(log *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result interface{}
	err := s.db.Update(func(txn *badger.Txn) error {
		switch log.Type {
		case raft.LogCommand:
			// Parse the command
			cmd, err := s.parseCommand(log.Data)
			if err != nil {
				return err
			}

			switch cmd.Op {
			case "SET":
				err = txn.Set([]byte(cmd.Key), cmd.Value)
				result = "OK"
			case "DEL":
				err = txn.Delete([]byte(cmd.Key))
				result = "OK"
			default:
				return fmt.Errorf("unknown command: %s", cmd.Op)
			}
		}
		return nil
	})

	if err != nil {
		s.log.Errorf("Failed to apply log entry: %v", err)
		return err
	}

	return result
}

// Snapshot returns a snapshot of the FSM
func (s *BadgerStore) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &BadgerSnapshot{
		db:  s.db,
		log: s.log,
	}, nil
}

// Restore restores the FSM from a snapshot
func (s *BadgerStore) Restore(rc io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear existing data
	err := s.db.DropAll()
	if err != nil {
		return fmt.Errorf("failed to clear existing data: %v", err)
	}

	// Read snapshot data
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read snapshot data: %v", err)
	}

	// Parse and restore data
	err = s.db.Update(func(txn *badger.Txn) error {
		offset := 0
		for offset < len(data) {
			if offset+8 > len(data) {
				return fmt.Errorf("invalid snapshot data")
			}

			// Read key length
			keyLen := binary.BigEndian.Uint32(data[offset:offset+4])
			offset += 4

			if offset+int(keyLen) > len(data) {
				return fmt.Errorf("invalid snapshot data")
			}

			// Read key
			key := data[offset : offset+int(keyLen)]
			offset += int(keyLen)

			if offset+4 > len(data) {
				return fmt.Errorf("invalid snapshot data")
			}

			// Read value length
			valueLen := binary.BigEndian.Uint32(data[offset:offset+4])
			offset += 4

			if offset+int(valueLen) > len(data) {
				return fmt.Errorf("invalid snapshot data")
			}

			// Read value
			value := data[offset : offset+int(valueLen)]
			offset += int(valueLen)

			// Set key-value pair
			err := txn.Set(key, value)
			if err != nil {
				return fmt.Errorf("failed to restore key-value pair: %v", err)
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to restore snapshot: %v", err)
	}

	s.log.Infof("Successfully restored snapshot with %d bytes", len(data))
	return nil
}

// Get retrieves a value by key
func (s *BadgerStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %v", key, err)
	}

	return value, nil
}

// Set sets a key-value pair
func (s *BadgerStore) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

// Delete deletes a key
func (s *BadgerStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Scan performs a range scan
func (s *BadgerStore) Scan(prefix string, limit int) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][]byte)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if limit > 0 && count >= limit {
				break
			}

			item := it.Item()
			key := string(item.Key())
			
					// Skip internal data (__dcache namespace)
		if strings.HasPrefix(key, InternalNamespace) {
			continue
		}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy value for key %s: %v", key, err)
			}

			result[key] = value
			count++
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to scan with prefix %s: %v", prefix, err)
	}

	return result, nil
}

// Close closes the BadgerDB
func (s *BadgerStore) Close() error {
	return s.db.Close()
}

// Command represents a cache operation
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// parseCommand parses a command from log data
func (s *BadgerStore) parseCommand(data []byte) (*Command, error) {
	// Simple command format: "OP:KEY:VALUE"
	parts := bytes.Split(data, []byte(":"))
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid command format")
	}

	cmd := &Command{
		Op:  string(parts[0]),
		Key: string(parts[1]),
	}

	if len(parts) > 2 {
		cmd.Value = parts[2]
	}

	return cmd, nil
}

// BadgerSnapshot implements raft.FSMSnapshot
type BadgerSnapshot struct {
	db  *badger.DB
	log *logrus.Logger
}

// Persist saves the snapshot to the given sink
func (s *BadgerSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Cancel()

	var buf bytes.Buffer
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			
			// Skip internal data (__dcache namespace)
			if bytes.HasPrefix(key, []byte(InternalNamespace)) {
				continue
			}
			
			value, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy value: %v", err)
			}

			// Write key length
			keyLen := uint32(len(key))
			if err := binary.Write(&buf, binary.BigEndian, keyLen); err != nil {
				return fmt.Errorf("failed to write key length: %v", err)
			}

			// Write key
			if _, err := buf.Write(key); err != nil {
				return fmt.Errorf("failed to write key: %v", err)
			}

			// Write value length
			valueLen := uint32(len(value))
			if err := binary.Write(&buf, binary.BigEndian, valueLen); err != nil {
				return fmt.Errorf("failed to write value length: %v", err)
			}

			// Write value
			if _, err := buf.Write(value); err != nil {
				return fmt.Errorf("failed to write value: %v", err)
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	// Write snapshot data to sink
	if _, err := sink.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write snapshot data: %v", err)
	}

	s.log.Infof("Created snapshot with %d bytes", buf.Len())
	return sink.Close()
}

// Release releases the snapshot
func (s *BadgerSnapshot) Release() {
	// Nothing to do for BadgerDB snapshots
}

// 实现 raft.LogStore 和 raft.StableStore

// --- raft.LogStore ---

func (s *BadgerStore) FirstIndex() (uint64, error) {
	var first uint64 = 0
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.HasPrefix(key, []byte(RaftWALNamespace+"/log-")) {
				idx := parseLogIndex(key)
				if first == 0 || idx < first {
					first = idx
				}
			}
		}
		return nil
	})
	return first, err
}

func (s *BadgerStore) LastIndex() (uint64, error) {
	var last uint64 = 0
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.HasPrefix(key, []byte(RaftWALNamespace+"/log-")) {
				idx := parseLogIndex(key)
				if idx > last {
					last = idx
				}
			}
		}
		return nil
	})
	return last, err
}

func (s *BadgerStore) GetLog(idx uint64, logEntry *raft.Log) error {
	return s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(idx))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return decodeLog(val, logEntry)
	})
}

func (s *BadgerStore) StoreLog(logEntry *raft.Log) error {
	data, err := encodeLog(logEntry)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(logKey(logEntry.Index), data)
	})
}

func (s *BadgerStore) StoreLogs(logs []*raft.Log) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, logEntry := range logs {
			data, err := encodeLog(logEntry)
			if err != nil {
				return err
			}
			if err := txn.Set(logKey(logEntry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStore) DeleteRange(min, max uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for i := min; i <= max; i++ {
			if err := txn.Delete(logKey(i)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}

// --- raft.StableStore ---
func (s *BadgerStore) SetStable(key []byte, val []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(stableKey(key), val)
	})
}

func (s *BadgerStore) GetStable(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(stableKey(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, err
}

func (s *BadgerStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return s.SetStable(key, buf)
}

func (s *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.GetStable(key)
	if err != nil || len(val) != 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(val), nil
}

// --- 辅助函数 ---
func logKey(idx uint64) []byte {
	return []byte(fmt.Sprintf("%s/log-%020d", RaftWALNamespace, idx))
}

func parseLogIndex(key []byte) uint64 {
	var idx uint64
	fmt.Sscanf(string(key), "%s/log-%d", RaftWALNamespace, &idx)
	return idx
}

func encodeLog(logEntry *raft.Log) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(logEntry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeLog(data []byte, logEntry *raft.Log) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	return dec.Decode(logEntry)
}

func stableKey(key []byte) []byte {
	return append([]byte(fmt.Sprintf("%s/stable-", RaftWALNamespace)), key...)
} 