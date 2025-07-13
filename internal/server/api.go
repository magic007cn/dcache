package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dcache/internal/config"
	"dcache/internal/raft"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// API represents the HTTP API server
type API struct {
	raftManager *raft.Manager
	config      *config.Config
	log         *logrus.Logger
	Router      *mux.Router
}

// NewAPI creates a new API server
func NewAPI(raftManager *raft.Manager, cfg *config.Config, log *logrus.Logger) *API {
	api := &API{
		raftManager: raftManager,
		config:      cfg,
		log:         log,
		Router:      mux.NewRouter(),
	}

	api.setupRoutes()
	return api
}

// setupRoutes sets up the API routes
func (api *API) setupRoutes() {
	// Cache operations
	api.Router.HandleFunc("/api/v1/get/{key}", api.handleGet).Methods("GET")
	api.Router.HandleFunc("/api/v1/set", api.handleSet).Methods("POST")
	api.Router.HandleFunc("/api/v1/delete/{key}", api.handleDelete).Methods("DELETE")
	api.Router.HandleFunc("/api/v1/scan", api.handleScan).Methods("GET")

	// Cluster operations
	api.Router.HandleFunc("/api/v1/cluster/status", api.handleClusterStatus).Methods("GET")
	api.Router.HandleFunc("/api/v1/cluster/leader", api.handleGetLeader).Methods("GET")
	api.Router.HandleFunc("/api/v1/cluster/transfer-leadership", api.handleTransferLeadership).Methods("POST")
	api.Router.HandleFunc("/api/v1/cluster/join", api.handleJoinCluster).Methods("POST")
	api.Router.HandleFunc("/api/v1/cluster/add-node", api.handleAddNode).Methods("POST")
	api.Router.HandleFunc("/api/v1/cluster/remove-node", api.handleRemoveNode).Methods("POST")

	// Health check
	api.Router.HandleFunc("/health", api.handleHealth).Methods("GET")
}

// Start starts the API server
func (api *API) Start(addr string) error {
	api.log.Infof("Starting API server on %s", addr)
	return http.ListenAndServe(addr, api.Router)
}

// Response represents a generic API response
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// SetRequest represents a set operation request
type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ScanRequest represents a scan operation request
type ScanRequest struct {
	Prefix string `json:"prefix"`
	Limit  int    `json:"limit"`
}

// ClusterStatus represents cluster status information
type ClusterStatus struct {
	NodeID    string `json:"node_id"`
	State     string `json:"state"`
	IsLeader  bool   `json:"is_leader"`
	Leader    string `json:"leader"`
	Term      uint64 `json:"term"`
	Index     uint64 `json:"index"`
}

// handleGet handles GET requests
func (api *API) handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward GET request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	value, err := api.raftManager.Get(key)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get key: %v", err))
		return
	}

	if value == "" {
		api.writeResponse(w, http.StatusNotFound, Response{
			Success: false,
			Error:   "Key not found",
		})
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data: map[string]string{
			"key":   key,
			"value": string(value),
		},
	})
}

// handleSet handles SET requests
func (api *API) handleSet(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader first
	if !api.raftManager.IsLeader() {
		// Forward to leader without parsing request body
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward SET request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Only parse request body if this is the leader
	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Key == "" {
		api.writeError(w, http.StatusBadRequest, "Key is required")
		return
	}

	err := api.raftManager.Set(req.Key, []byte(req.Value))
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to set key: %v", err))
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    "OK",
	})
}

// handleDelete handles DELETE requests
func (api *API) handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward DELETE request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	err := api.raftManager.Delete(key)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete key: %v", err))
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    "OK",
	})
}

// handleScan handles SCAN requests
func (api *API) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	limitStr := r.URL.Query().Get("limit")
	
	limit := 100 // default limit
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward SCAN request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	result, err := api.raftManager.Scan(prefix, limit)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to scan: %v", err))
		return
	}

	// Convert result to string map
	stringResult := make(map[string]string)
	for _, kv := range result {
		stringResult[kv.Key] = kv.Value
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data: map[string]interface{}{
			"prefix": prefix,
			"limit":  limit,
			"count":  len(stringResult),
			"items":  stringResult,
		},
	})
}

// handleClusterStatus handles cluster status requests
func (api *API) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	status := ClusterStatus{
		NodeID:   api.config.GetNodeID(),
		State:    api.raftManager.GetState().String(),
		IsLeader: api.raftManager.IsLeader(),
		Leader:   api.raftManager.GetLeader(),
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    status,
	})
}

// handleGetLeader handles get leader requests
func (api *API) handleGetLeader(w http.ResponseWriter, r *http.Request) {
	leader := api.raftManager.GetLeader()
	
	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data: map[string]string{
			"leader": leader,
		},
	})
}

// handleTransferLeadership handles leadership transfer requests
func (api *API) handleTransferLeadership(w http.ResponseWriter, r *http.Request) {
	targetID := r.URL.Query().Get("target")
	targetAddr := r.URL.Query().Get("address")
	if targetID == "" || targetAddr == "" {
		api.writeError(w, http.StatusBadRequest, "target and address parameters are required")
		return
	}

	err := api.raftManager.TransferLeadership(targetID, targetAddr)
	if err != nil {
		if strings.Contains(err.Error(), "not leader") {
			api.writeError(w, http.StatusServiceUnavailable, "Not leader")
			return
		}
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to transfer leadership: %v", err))
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    "Leadership transfer initiated",
	})
}

// handleJoinCluster handles join cluster requests
func (api *API) handleJoinCluster(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward JOIN request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid form data")
		return
	}

	nodeID := r.FormValue("node_id")
	address := r.FormValue("address")

	if nodeID == "" || address == "" {
		api.writeError(w, http.StatusBadRequest, "node_id and address are required")
		return
	}

	// Add the new node to the cluster
	err := api.raftManager.AddVoter(nodeID, address)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add node: %v", err))
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    fmt.Sprintf("Successfully added node %s", nodeID),
	})
}

// handleAddNode handles add node requests
func (api *API) handleAddNode(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward ADD_NODE request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Parse JSON request
	var req struct {
		NodeID  string `json:"node_id"`
		Address string `json:"address"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if req.NodeID == "" || req.Address == "" {
		api.writeError(w, http.StatusBadRequest, "node_id and address are required")
		return
	}

	// Add the new node to the cluster
	err := api.raftManager.AddVoter(req.NodeID, req.Address)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to add node: %v", err))
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    fmt.Sprintf("Successfully added node %s", req.NodeID),
	})
}

// handleRemoveNode handles remove node requests
func (api *API) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward REMOVE_NODE request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Parse JSON request
	var req struct {
		NodeID string `json:"node_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if req.NodeID == "" {
		api.writeError(w, http.StatusBadRequest, "node_id is required")
		return
	}

	// Remove the node from the cluster
	err := api.raftManager.RemoveServer(req.NodeID)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to remove node: %v", err))
		return
	}

	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    fmt.Sprintf("Successfully removed node %s", req.NodeID),
	})
}

// handleHealth handles health check requests
func (api *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	api.writeResponse(w, http.StatusOK, Response{
		Success: true,
		Data:    "OK",
	})
}

// writeResponse writes a JSON response
func (api *API) writeResponse(w http.ResponseWriter, statusCode int, response Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

// forwardToLeader forwards the request to the leader node
func (api *API) forwardToLeader(w http.ResponseWriter, r *http.Request) error {
	leader := api.raftManager.GetLeader()
	if leader == "" {
		return fmt.Errorf("no leader available")
	}

	// Parse leader address and convert Raft address to HTTP address
	leaderAddr := string(leader)
	
	// Convert Raft address (e.g., 127.0.0.1:9091) to HTTP address (127.0.0.1:8081)
	// Extract IP from Raft address
	parts := strings.Split(leaderAddr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid leader address format: %s", leaderAddr)
	}
	
	// Determine HTTP port based on node ID
	var httpPort string
	switch {
	case strings.Contains(leaderAddr, "9091"):
		httpPort = "8081"
	case strings.Contains(leaderAddr, "9092"):
		httpPort = "8082"
	case strings.Contains(leaderAddr, "9093"):
		httpPort = "8083"
	default:
		httpPort = "8081" // Default
	}
	
	httpAddr := fmt.Sprintf("%s:%s", parts[0], httpPort)

	// Create HTTP client
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Read and buffer the request body
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	// Create new request with buffered body
	var req *http.Request
	var err error
	if len(bodyBytes) > 0 {
		req, err = http.NewRequest(r.Method, fmt.Sprintf("http://%s%s", httpAddr, r.URL.Path), bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}
	} else {
		req, err = http.NewRequest(r.Method, fmt.Sprintf("http://%s%s", httpAddr, r.URL.Path), nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}
	}

	// Copy headers
	for name, values := range r.Header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}

	// Copy query parameters
	req.URL.RawQuery = r.URL.RawQuery

	// Forward request
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to forward request: %v", err)
	}
	defer resp.Body.Close()

	// Copy response headers
	for name, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}

	// Copy response status and body
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

// writeError writes an error response
func (api *API) writeError(w http.ResponseWriter, statusCode int, message string) {
	api.writeResponse(w, statusCode, Response{
		Success: false,
		Error:   message,
	})
} 