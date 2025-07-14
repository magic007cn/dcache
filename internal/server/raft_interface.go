package server

import (
	"dcache/internal/storage"
	"github.com/hashicorp/raft"
)

// RaftManager defines the interface for Raft operations
type RaftManager interface {
	// Apply applies a command to the Raft cluster
	Apply(data []byte) error
	
	// Get retrieves a value by key
	Get(key string) (string, error)
	
	// Set sets a value in the store (only on leader)
	Set(key string, value []byte) error
	
	// BatchSet applies a batch set command to the Raft cluster (strong consistency)
	BatchSet(pairs []storage.KeyValue) error
	
	// Scan retrieves key-value pairs with a prefix
	Scan(prefix string, limit int) ([]storage.KeyValue, error)
	
	// Delete deletes a key from the store (only on leader)
	Delete(key string, errorOnNotExists bool) error
	
	// GetLeader returns the current leader address
	GetLeader() string
	
	// IsLeader checks if this node is the leader
	IsLeader() bool
	
	// GetNodes returns all node addresses
	GetNodes() []string
	
	// IsHealthy checks if the Raft cluster is healthy
	IsHealthy() bool
	
	// GetConfig returns the configuration
	GetConfig() interface{}
	
	// GetConfiguration returns the current cluster configuration
	GetConfiguration() raft.Configuration
} 