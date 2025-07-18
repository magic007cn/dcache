package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config represents the server configuration
type Config struct {
	NodeID              string   `mapstructure:"node-id"`
	ClusterID           string   `mapstructure:"cluster-id"`
	DataDir             string   `mapstructure:"data-dir"`
	StorageMode         string   `mapstructure:"storage-mode"`
	ListenClientURLs    string   `mapstructure:"listen-client-urls"`
	AdvertiseClientURLs string   `mapstructure:"advertise-client-urls"`
	ListenPeerURLs      string   `mapstructure:"listen-peer-urls"`
	InitialAdvertisePeerURLs string `mapstructure:"initial-advertise-peer-urls"`
	InitialCluster      string   `mapstructure:"initial-cluster"`
	LogLevel            string   `mapstructure:"log-level"`
	RaftLogLevel        string   `mapstructure:"raft-log-level"`
	GrpcPort            int      `mapstructure:"grpc-port"`
}

// Load loads configuration from file or environment variables
func Load(configFile string) (*Config, error) {
	v := viper.GetViper()

	// Set defaults
	v.SetDefault("node-id", "node1")
	v.SetDefault("cluster-id", "dcache-cluster")
	v.SetDefault("data-dir", "./data")
	v.SetDefault("storage-mode", "inmemory")
	v.SetDefault("listen-client-urls", "http://127.0.0.1:8080")
	v.SetDefault("advertise-client-urls", "http://127.0.0.1:8080")
	v.SetDefault("listen-peer-urls", "tcp://127.0.0.1:9091")
	v.SetDefault("initial-advertise-peer-urls", "tcp://127.0.0.1:9091")
	v.SetDefault("log-level", "warn")
	v.SetDefault("raft-log-level", "warn")
	v.SetDefault("grpc-port", 50051)

	// Read from command line flags
	v.BindEnv("node-id", "NODE_ID")
	v.BindEnv("cluster-id", "CLUSTER_ID")
	v.BindEnv("data-dir", "DATA_DIR")
	v.BindEnv("storage-mode", "STORAGE_MODE")
	v.BindEnv("listen-client-urls", "LISTEN_CLIENT_URLS")
	v.BindEnv("advertise-client-urls", "ADVERTISE_CLIENT_URLS")
	v.BindEnv("listen-peer-urls", "LISTEN_PEER_URLS")
	v.BindEnv("initial-advertise-peer-urls", "INITIAL_ADVERTISE_PEER_URLS")
	v.BindEnv("initial-cluster", "INITIAL_CLUSTER")
	v.BindEnv("log-level", "LOG_LEVEL")
	v.BindEnv("raft-log-level", "RAFT_LOG_LEVEL")
	v.BindEnv("grpc-port", "GRPC_PORT")

	// Read from config file if specified
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %v", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	return &cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("cluster-id is required")
	}

	if c.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	// Validate storage mode
	if c.StorageMode != "inmemory" && c.StorageMode != "persistent" {
		return fmt.Errorf("storage-mode must be either 'inmemory' or 'persistent', got: %s", c.StorageMode)
	}

	if c.ListenClientURLs == "" {
		return fmt.Errorf("listen-client-urls is required")
	}

	if c.ListenPeerURLs == "" {
		return fmt.Errorf("listen-peer-urls is required")
	}

	// Create data directory if it doesn't exist (only for persistent mode)
	if c.StorageMode == "persistent" {
		if err := os.MkdirAll(c.DataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %v", err)
		}
	}

	return nil
}

// GetNodeID extracts node ID from listen-peer-urls
func (c *Config) GetNodeID() string {
	return c.NodeID
}

// GetInitialClusterMap returns a map of node ID to peer URL
func (c *Config) GetInitialClusterMap() map[string]string {
	if c.InitialCluster == "" {
		return nil
	}

	clusterMap := make(map[string]string)
	pairs := strings.Split(c.InitialCluster, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			nodeID := strings.TrimSpace(parts[0])
			peerURL := strings.TrimSpace(parts[1])
			// Remove all possible prefixes
			peerURL = strings.TrimPrefix(peerURL, "http://")
			peerURL = strings.TrimPrefix(peerURL, "https://")
			peerURL = strings.TrimPrefix(peerURL, "tcp://")
			clusterMap[nodeID] = peerURL
		}
	}
	return clusterMap
} 