package server

import (
	"dcache/internal/storage"
)

// RaftManager defines the interface for Raft operations
type RaftManager interface {
	// Apply applies a command to the Raft cluster
	Apply(data []byte) error
	
	// Get retrieves a value by key
	Get(key string) (string, error)
	
	// Scan retrieves key-value pairs with a prefix
	Scan(prefix string, limit int) ([]storage.KeyValue, error)
	
	// GetLeader returns the current leader address
	GetLeader() string
	
	// IsLeader checks if this node is the leader
	IsLeader() bool
	
	// GetNodes returns all node addresses
	GetNodes() []string
	
	// IsHealthy checks if the Raft cluster is healthy
	IsHealthy() bool
} 