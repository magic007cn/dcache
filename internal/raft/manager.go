package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"dcache/internal/config"
	"dcache/internal/storage"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Manager manages the Raft cluster
type Manager struct {
	raft     *raft.Raft
	store    *storage.BadgerStore
	transport *raft.NetworkTransport
	config   *config.Config
	log      *logrus.Logger
}

// NewManager creates a new Raft manager
func NewManager(cfg *config.Config, log *logrus.Logger) (*Manager, error) {
	// Create storage
	store, err := storage.NewBadgerStore(cfg.DataDir, cfg.StorageMode, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %v", err)
	}

	// Create raft network transport
	peerAddr := strings.TrimPrefix(cfg.ListenPeerURLs, "tcp://")
	addr, err := net.ResolveTCPAddr("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve peer address: %v", err)
	}
	transport, err := raft.NewTCPTransport(peerAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft network transport: %v", err)
	}

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.GetNodeID())
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 1000
	raftConfig.HeartbeatTimeout = 2000 * time.Millisecond
	raftConfig.ElectionTimeout = 5000 * time.Millisecond
	raftConfig.CommitTimeout = 5000 * time.Millisecond
	raftConfig.MaxAppendEntries = 64
	raftConfig.ShutdownOnRemove = false
	
	// Set Raft log level from config
	raftLogLevel := strings.ToUpper(cfg.RaftLogLevel)
	switch raftLogLevel {
	case "DEBUG", "INFO", "WARN", "ERROR":
		raftConfig.LogLevel = raftLogLevel
	default:
		raftConfig.LogLevel = "WARN"
	}

	// Create log store 和 stable store 都用 BadgerStore
	logStore := store
	stableStore := &stableStoreWrapper{store: store}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, store, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %v", err)
	}

	return &Manager{
		raft:     r,
		store:    store,
		transport: transport,
		config:   cfg,
		log:      log,
	}, nil
}

// Start starts the Raft manager
func (m *Manager) Start() error {
	// 等待 Raft 初始化完成
	time.Sleep(100 * time.Millisecond)
	
	// 检查是否已经有集群配置
	confFuture := m.raft.GetConfiguration()
	if err := confFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}
	
	existingServers := confFuture.Configuration().Servers
	m.log.Infof("Found %d existing servers in cluster", len(existingServers))
	
	// 如果已经有服务器配置，说明集群已经存在，不需要bootstrap
	if len(existingServers) > 0 {
		m.log.Info("Cluster already exists, skipping bootstrap")
	} else if m.config.InitialCluster != "" {
		// 只有在没有现有配置且有初始集群配置时才bootstrap
		m.log.Info("No existing cluster found, bootstrapping new cluster")
		bootstrapErr := m.bootstrapCluster()
		if bootstrapErr != nil {
			// 如果bootstrap失败，可能是因为集群已经存在，记录警告但继续
			m.log.Warnf("Bootstrap failed (this is normal if cluster already exists): %v", bootstrapErr)
		}
		
		// 对于单节点模式，如果bootstrap失败，尝试强制重新bootstrap
		if bootstrapErr != nil && len(m.config.GetInitialClusterMap()) == 1 {
			m.log.Info("Single node mode detected, attempting to force bootstrap")
			// 等待一段时间让raft稳定
			time.Sleep(500 * time.Millisecond)
			if err := m.bootstrapCluster(); err != nil {
				m.log.Errorf("Force bootstrap failed: %v", err)
			} else {
				m.log.Info("Force bootstrap successful")
			}
		}
	} else {
		m.log.Info("No initial cluster configuration, waiting to join existing cluster")
	}

	m.log.Infof("Raft manager started, node ID: %s", m.config.GetNodeID())
	return nil
}

// Stop stops the Raft manager
func (m *Manager) Stop() error {
	if m.raft != nil {
		if err := m.raft.Shutdown().Error(); err != nil {
			m.log.Errorf("Failed to shutdown raft: %v", err)
		}
	}

	if m.transport != nil {
		m.transport.Close()
	}

	if m.store != nil {
		if err := m.store.Close(); err != nil {
			m.log.Errorf("Failed to close store: %v", err)
		}
	}

	return nil
}

// bootstrapCluster bootstraps the Raft cluster
func (m *Manager) bootstrapCluster() error {
	// Parse initial cluster configuration
	clusterMap := m.config.GetInitialClusterMap()
	if len(clusterMap) == 0 {
		return fmt.Errorf("no initial cluster configuration")
	}

	// Create server configurations
	var servers []raft.Server
	for nodeID, peerURL := range clusterMap {
		// Remove tcp:// prefix if present
		cleanAddr := strings.TrimPrefix(peerURL, "tcp://")
		server := raft.Server{
			ID:      raft.ServerID(nodeID),
			Address: raft.ServerAddress(cleanAddr),
		}
		servers = append(servers, server)
	}

	// Bootstrap the cluster
	future := m.raft.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %v", err)
	}

	m.log.Infof("Successfully bootstrapped cluster with %d nodes", len(servers))
	return nil
}

// JoinCluster joins an existing cluster
func (m *Manager) JoinCluster(leaderAddr string) error {
	if m.raft.State() == raft.Leader {
		return fmt.Errorf("cannot join cluster as leader")
	}

	// Create a connection to the leader
	conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %v", err)
	}
	defer conn.Close()

	// Call the join API on the leader
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.PostForm(fmt.Sprintf("http://%s/api/v1/cluster/join", leaderAddr), url.Values{
		"node_id": {m.config.GetNodeID()},
		"address": {m.config.ListenPeerURLs},
	})
	if err != nil {
		return fmt.Errorf("failed to join cluster: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to join cluster: status %d", resp.StatusCode)
	}

	m.log.Infof("Successfully joined cluster via leader %s", leaderAddr)
	return nil
}

// GetLeader returns the current leader
func (m *Manager) GetLeader() string {
	return string(m.raft.Leader())
}

// IsLeader checks if this node is the leader
func (m *Manager) IsLeader() bool {
	return m.raft.State() == raft.Leader
}

// GetState returns the current Raft state
func (m *Manager) GetState() raft.RaftState {
	return m.raft.State()
}

// GetConfiguration returns the current cluster configuration
func (m *Manager) GetConfiguration() raft.Configuration {
	confFuture := m.raft.GetConfiguration()
	if err := confFuture.Error(); err != nil {
		m.log.Errorf("Failed to get configuration: %v", err)
		return raft.Configuration{}
	}
	return confFuture.Configuration()
}

// AddVoter adds a voter to the cluster
func (m *Manager) AddVoter(id string, address string) error {
	if !m.IsLeader() {
		return fmt.Errorf("not leader")
	}

	// Clean the address (remove tcp:// prefix if present)
	cleanAddr := strings.TrimPrefix(address, "tcp://")
	
	future := m.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(cleanAddr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter: %v", err)
	}

	m.log.Infof("Successfully added voter %s at %s", id, cleanAddr)
	return nil
}

// RemoveServer removes a server from the cluster
func (m *Manager) RemoveServer(id string) error {
	if !m.IsLeader() {
		return fmt.Errorf("not leader")
	}

	// Check if we're trying to remove ourselves
	if id == m.config.GetNodeID() {
		return fmt.Errorf("cannot remove self from cluster")
	}

	future := m.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %v", err)
	}

	m.log.Infof("Successfully removed server %s", id)
	return nil
}

// ApplyCommand applies a command to the cluster
func (m *Manager) ApplyCommand(cmd *storage.Command) (interface{}, error) {
	// Serialize command
	data := fmt.Sprintf("%s:%s", cmd.Op, cmd.Key)
	if cmd.Value != nil {
		data = fmt.Sprintf("%s:%s:%s", cmd.Op, cmd.Key, string(cmd.Value))
	}

	// Apply to Raft
	future := m.raft.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("failed to apply command: %v", err)
	}

	return future.Response(), nil
}

// BatchSet applies a batch set command to the Raft cluster (strong consistency)
func (m *Manager) BatchSet(pairs []storage.KeyValue) error {
	if !m.IsLeader() {
		return fmt.Errorf("not leader")
	}
	// gob 编码 pairs
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(pairs); err != nil {
		return fmt.Errorf("failed to encode batch: %v", err)
	}
	cmd := &storage.Command{
		Op:   "BATCH_SET",
		Data: buf.Bytes(),
	}
	// gob 编码 Command
	var cmdBuf bytes.Buffer
	if err := gob.NewEncoder(&cmdBuf).Encode(cmd); err != nil {
		return fmt.Errorf("failed to encode command: %v", err)
	}
	future := m.raft.Apply(cmdBuf.Bytes(), 5*time.Second)
	return future.Error()
}

// Get retrieves a value from the store
func (m *Manager) Get(key string) (string, error) {
	data, err := m.store.Get(key)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Set sets a value in the store (only on leader)
func (m *Manager) Set(key string, value []byte) error {
	if !m.IsLeader() {
		return fmt.Errorf("not leader")
	}

	cmd := &storage.Command{
		Op:    "SET",
		Key:   key,
		Value: value,
	}

	_, err := m.ApplyCommand(cmd)
	return err
}

// Delete deletes a key from the store (only on leader)
func (m *Manager) Delete(key string, errorOnNotExists bool) error {
	if !m.IsLeader() {
		return fmt.Errorf("not leader")
	}

	// 直接调用存储层，避免通过 Raft 共识（因为 errorOnNotExists 是本地参数）
	return m.store.Delete(key, errorOnNotExists)
}

// Scan performs a range scan
func (m *Manager) Scan(prefix string, limit int) ([]storage.KeyValue, error) {
	pairs, err := m.store.Scan(prefix, limit)
	if err != nil {
		return nil, err
	}
	
	var result []storage.KeyValue
	for key, value := range pairs {
		result = append(result, storage.KeyValue{
			Key:   key,
			Value: string(value),
		})
	}
	return result, nil
}

// TransferLeadership transfers leadership to another node
func (m *Manager) TransferLeadership(targetID string, targetAddr string) error {
	if !m.IsLeader() {
		return fmt.Errorf("not leader")
	}
	future := m.raft.LeadershipTransferToServer(raft.ServerID(targetID), raft.ServerAddress(targetAddr))
	return future.Error()
}

// Apply applies data to the Raft cluster
func (m *Manager) Apply(data []byte) error {
	future := m.raft.Apply(data, 5*time.Second)
	return future.Error()
}

// GetNodes returns all node addresses
func (m *Manager) GetNodes() []string {
	conf := m.GetConfiguration()
	var nodes []string
	for _, server := range conf.Servers {
		nodes = append(nodes, string(server.Address))
	}
	return nodes
}

// IsHealthy checks if the Raft cluster is healthy
func (m *Manager) IsHealthy() bool {
	return m.raft.State() != raft.Shutdown
}

// GetConfig returns the configuration
func (m *Manager) GetConfig() interface{} {
	return m.config
}



// stableStoreWrapper wraps BadgerStore to implement raft.StableStore
type stableStoreWrapper struct {
	store *storage.BadgerStore
}

func (w *stableStoreWrapper) Set(key []byte, val []byte) error {
	return w.store.SetStable(key, val)
}

func (w *stableStoreWrapper) Get(key []byte) ([]byte, error) {
	return w.store.GetStable(key)
}

func (w *stableStoreWrapper) SetUint64(key []byte, val uint64) error {
	return w.store.SetUint64(key, val)
}

func (w *stableStoreWrapper) GetUint64(key []byte) (uint64, error) {
	return w.store.GetUint64(key)
} 