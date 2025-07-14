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
	"dcache/internal/storage"

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
	// KV operations (RESTful)
	api.Router.HandleFunc("/api/v1/keys/{key}", api.handleGetKey).Methods("GET")
	api.Router.HandleFunc("/api/v1/keys/{key}", api.handlePutKey).Methods("PUT")
	api.Router.HandleFunc("/api/v1/keys/{key}", api.handleDelete).Methods("DELETE")
	api.Router.HandleFunc("/api/v1/keys", api.handleRangeScan).Methods("GET")
	
	// Batch operations with query parameter to distinguish from range scan
	api.Router.HandleFunc("/api/v1/keys", api.handleBatchSet).Methods("POST")
	api.Router.HandleFunc("/api/v1/keys", api.handleBatchSet).Methods("PUT")
	api.Router.HandleFunc("/api/v1/keys", api.handleBatchGet).Methods("GET")
	api.Router.HandleFunc("/api/v1/keys", api.handleBatchDelete).Methods("DELETE")

	// Node operations
	api.Router.HandleFunc("/api/v1/node/health", api.handleHealth).Methods("GET")
	api.Router.HandleFunc("/api/v1/node/leader", api.handleGetLeader).Methods("GET")
	api.Router.HandleFunc("/api/v1/node/transfer-leadership", api.handleTransferLeadership).Methods("POST")

	// Cluster operations
	api.Router.HandleFunc("/api/v1/cluster/status", api.handleClusterStatus).Methods("GET")
	api.Router.HandleFunc("/api/v1/cluster/join", api.handleJoinCluster).Methods("POST")
	api.Router.HandleFunc("/api/v1/cluster/add-node", api.handleAddNode).Methods("POST")
	api.Router.HandleFunc("/api/v1/cluster/remove-node", api.handleRemoveNode).Methods("POST")
	api.Router.HandleFunc("/api/v1/cluster/members", api.handleGetMembers).Methods("GET")

	// Legacy batch operations (for backward compatibility)
	api.Router.HandleFunc("/api/v1/batch/set", api.handleBatchSet).Methods("POST")
	api.Router.HandleFunc("/api/v1/batch/get", api.handleBatchGet).Methods("POST")
	api.Router.HandleFunc("/api/v1/batch/delete", api.handleBatchDelete).Methods("POST")
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

// KVResponse represents a KV operation response (without success field)
type KVResponse struct {
	Data interface{} `json:"data,omitempty"`
	Error string     `json:"error,omitempty"`
}

// KVHeader represents KV operation response header
type KVHeader struct {
	ClusterID string `json:"cluster_id"`
	NodeID    string `json:"node_id"`
	Revision  uint64 `json:"revision"`
	RaftTerm  uint64 `json:"raft_term"`
}

// KVItem represents key-value pair
type KVItem struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	CreateRevision uint64 `json:"create_revision"`
	ModRevision    uint64 `json:"mod_revision"`
	Version        uint64 `json:"version"`
}

// GetResponse represents GET response
type GetResponse struct {
	Header       KVHeader   `json:"header"`
	KVs          []KVItem   `json:"kvs"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	Errors       []string   `json:"errors,omitempty"`
	Code         int        `json:"code"`
	Error        string     `json:"error,omitempty"`
}

// PutResponse represents PUT/SET response
type PutResponse struct {
	Header       KVHeader   `json:"header"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	Errors       []string   `json:"errors,omitempty"`
	Code         int        `json:"code"`
	Error        string     `json:"error,omitempty"`
}

// DeleteResponse represents DELETE response
type DeleteResponse struct {
	Header       KVHeader   `json:"header"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	Errors       []string   `json:"errors,omitempty"`
	Code         int        `json:"code"`
	Error        string     `json:"error,omitempty"`
}

// ScanResponse represents SCAN response
type ScanResponse struct {
	Header       KVHeader   `json:"header"`
	KVs          []KVItem   `json:"kvs"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	Errors       []string   `json:"errors,omitempty"`
	Code         int        `json:"code"`
	Error        string     `json:"error,omitempty"`
}

// ErrorResponse represents error response
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// 应用级别的错误码定义
const (
	ErrorCodeOK              = 0
	ErrorCodeInvalidArgument = 3
	ErrorCodeNotFound        = 5
	ErrorCodeNotLeader       = 9
	ErrorCodeInternal        = 13
	ErrorCodeBatchPartial    = 100
)

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

// ClusterMember represents a cluster member
type ClusterMember struct {
	NodeID   string `json:"node_id"`
	NodeName string `json:"node_name"`
	Role     string `json:"role"`
	Address  string `json:"address"`
}

// Batch operations
type BatchSetRequest struct {
	Pairs []KeyValue `json:"pairs"`
}

type BatchGetRequest struct {
	Keys []string `json:"keys"`
}

type BatchDeleteRequest struct {
	Keys []string `json:"keys"`
	ErrorOnNotExists bool `json:"error_on_not_exists"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type BatchResponse struct {
	Header       KVHeader   `json:"header"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	Errors       []string   `json:"errors,omitempty"`
	Data         interface{} `json:"data,omitempty"`
	Code         int        `json:"code"` // Added for partial success
}

// RangeScanResponse represents RANGE SCAN response
type RangeScanResponse struct {
	Header       KVHeader   `json:"header"`
	KVs          []KVItem   `json:"kvs"`
	SuccessCount int        `json:"success_count"`
	ErrorCount   int        `json:"error_count"`
	Errors       []string   `json:"errors,omitempty"`
	Code         int        `json:"code"`
	Error        string     `json:"error,omitempty"`
}

// RangeScanRequest represents a range scan operation request
type RangeScanRequest struct {
	Prefix string `json:"prefix"`
	Limit  int    `json:"limit"`
}

// handleGetKey godoc
// @Summary 获取单个key
// @Description 获取指定key的值
// @Tags KV
// @Accept  json
// @Produce  json
// @Param   key  path  string  true  "键"
// @Success 200 {object} GetResponse
// @Failure 404 {object} ErrorResponse
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/keys/{key} [get]
func (api *API) handleGetKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if !api.raftManager.IsLeader() {
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward GET request to leader: %v", err)
			api.writeKVError(w, http.StatusServiceUnavailable, "Failed to forward to leader")
		}
		return
	}

	value, err := api.raftManager.Get(key)
	if err != nil {
		api.writeKVError(w, http.StatusInternalServerError, "Failed to get key")
		return
	}

	if value == "" {
		api.writeKVError(w, http.StatusNotFound, "Key not found")
		return
	}

	header := api.getKVHeader()
	response := GetResponse{
		Header:       header,
		KVs:          []KVItem{{Key: key, Value: value, CreateRevision: 0, ModRevision: 0, Version: 1}},
		SuccessCount: 1,
		ErrorCount:   0,
		Code:         ErrorCodeOK,
	}
	api.writeKVResponse(w, http.StatusOK, response)
}

// handlePutKey godoc
// @Summary 写入/更新单个key
// @Description 写入/更新指定key的值
// @Tags KV
// @Accept  json
// @Produce  json
// @Param   key  path  string  true  "键"
// @Param   value body SetRequest true "值"
// @Success 200 {object} PutResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/keys/{key} [put]
func (api *API) handlePutKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	if !api.raftManager.IsLeader() {
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward PUT request to leader: %v", err)
			api.writeKVError(w, http.StatusServiceUnavailable, "Failed to forward to leader")
		}
		return
	}

	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeKVError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Value == "" {
		api.writeKVError(w, http.StatusBadRequest, "Value is required")
		return
	}

	err := api.raftManager.Set(key, []byte(req.Value))
	if err != nil {
		api.writeKVError(w, http.StatusInternalServerError, "Failed to set key")
		return
	}

	header := api.getKVHeader()
	response := PutResponse{
		Header:       header,
		SuccessCount: 1,
		ErrorCount:   0,
		Code:         ErrorCodeOK,
	}
	api.writeKVResponse(w, http.StatusOK, response)
}

// handleDelete godoc
// @Summary 删除单个key
// @Description 删除指定key
// @Tags KV
// @Accept  json
// @Produce  json
// @Param   key  path  string  true  "键"
// @Success 200 {object} DeleteResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/keys/{key} [delete]
func (api *API) handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	errorOnNotExists := false
	if errorOnNotExistsStr := r.URL.Query().Get("error_on_not_exists"); errorOnNotExistsStr != "" {
		if errorOnNotExistsStr == "true" || errorOnNotExistsStr == "1" {
			errorOnNotExists = true
		}
	}

	if !api.raftManager.IsLeader() {
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward DELETE request to leader: %v", err)
			api.writeKVError(w, http.StatusServiceUnavailable, "Failed to forward to leader")
		}
		return
	}

	err := api.raftManager.Delete(key, errorOnNotExists)
	if err != nil {
		if err.Error() == "key not found" {
			api.writeKVError(w, http.StatusNotFound, "Key not found")
		} else {
			api.writeKVError(w, http.StatusInternalServerError, "Failed to delete key")
		}
		return
	}

	header := api.getKVHeader()
	response := DeleteResponse{
		Header:       header,
		SuccessCount: 1,
		ErrorCount:   0,
		Code:         ErrorCodeOK,
	}
	api.writeKVResponse(w, http.StatusOK, response)
}

// handleRangeScan handles GET /api/v1/keys?prefix=xxx&limit=xxx
func (api *API) handleRangeScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if !api.raftManager.IsLeader() {
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward RANGE SCAN request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}
	result, err := api.raftManager.Scan(prefix, limit)
	if err != nil {
		api.writeKVError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to range scan: %v", err))
		return
	}
	var kvs []KVItem
	for _, kv := range result {
		kvs = append(kvs, KVItem{Key: kv.Key, Value: kv.Value, CreateRevision: 0, ModRevision: 0, Version: 1})
	}
	header := api.getKVHeader()
	response := RangeScanResponse{
		Header:       header, 
		KVs:          kvs, 
		SuccessCount: len(kvs),
		ErrorCount:   0,
		Code:         ErrorCodeOK,
	}
	api.writeKVResponse(w, http.StatusOK, response)
}

// handleClusterStatus handles cluster status requests
func (api *API) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	header := api.getKVHeader()
	leader := api.raftManager.GetLeader()
	nodes := api.raftManager.GetNodes()
	isLeader := api.raftManager.IsLeader()

	// 构建节点列表
	var nodeList []string
	for _, node := range nodes {
		nodeList = append(nodeList, string(node))
	}

	response := struct {
		Header   KVHeader `json:"header"`
		Leader   string   `json:"leader"`
		Nodes    []string `json:"nodes"`
		IsLeader bool     `json:"is_leader"`
	}{
		Header:   header,
		Leader:   leader,
		Nodes:    nodeList,
		IsLeader: isLeader,
	}

	api.writeKVResponse(w, http.StatusOK, response)
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

// handleBatchSet godoc
// @Summary 批量写入/更新
// @Description 批量写入/更新多个key
// @Tags KV
// @Accept  json
// @Produce  json
// @Param   pairs  body  []KeyValue  true  "键值对数组"
// @Success 200 {object} BatchResponse
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/keys [post]
func (api *API) handleBatchSet(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward BATCH_SET request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Parse JSON request
	var req BatchSetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if len(req.Pairs) == 0 {
		api.writeError(w, http.StatusBadRequest, "pairs array cannot be empty")
		return
	}

	// 类型转换
	pairs := make([]storage.KeyValue, len(req.Pairs))
	for i, kv := range req.Pairs {
		pairs[i] = storage.KeyValue{Key: kv.Key, Value: kv.Value}
	}
	err := api.raftManager.BatchSet(pairs)
	successCount := 0
	errorCount := 0
	var errors []string
	if err != nil {
		errors = append(errors, err.Error())
		errorCount = len(req.Pairs)
	} else {
		successCount = len(req.Pairs)
	}

	// 创建KV操作响应
	header := api.getKVHeader()
	response := BatchResponse{
		Header:       header,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
		Errors:       errors,
	}

	statusCode := http.StatusOK
	respCode := ErrorCodeOK
	if errorCount > 0 {
		statusCode = http.StatusPartialContent
		respCode = ErrorCodeBatchPartial
	}
	response.Code = respCode
	api.writeKVResponse(w, statusCode, response)
}

// handleBatchGet handles GET /api/v1/keys (batch get or range scan)
func (api *API) handleBatchGet(w http.ResponseWriter, r *http.Request) {
	// Check if this is a batch get (has request body) or range scan (has query params)
	if r.Body != nil && r.ContentLength > 0 {
		// This is a batch get operation
		api.handleBatchGetOperation(w, r)
	} else {
		// This is a range scan operation
		api.handleRangeScan(w, r)
	}
}

// handleBatchGetOperation godoc
// @Summary 批量获取
// @Description 批量获取多个key
// @Tags KV
// @Accept  json
// @Produce  json
// @Param   keys  body  []string  true  "key数组"
// @Success 200 {object} BatchResponse
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/keys [get]
func (api *API) handleBatchGetOperation(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward BATCH_GET request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Parse JSON request
	var req BatchGetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if len(req.Keys) == 0 {
		api.writeError(w, http.StatusBadRequest, "keys array cannot be empty")
		return
	}

	var pairs []KeyValue
	var errors []string
	successCount := 0
	errorCount := 0

	// Get all values
	for _, key := range req.Keys {
		if key == "" {
			errors = append(errors, "key cannot be empty")
			errorCount++
			continue
		}

		value, err := api.raftManager.Get(key)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get %s: %v", key, err))
			errorCount++
		} else if value == "" {
			errors = append(errors, fmt.Sprintf("key %s not found", key))
			errorCount++
		} else {
			pairs = append(pairs, KeyValue{Key: key, Value: value})
			successCount++
		}
	}

	// 创建KV操作响应
	header := api.getKVHeader()
	response := BatchResponse{
		Header:       header,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
		Errors:       errors,
		Data:         pairs,
	}

	statusCode := http.StatusOK
	respCode := ErrorCodeOK
	if errorCount > 0 {
		statusCode = http.StatusPartialContent
		respCode = ErrorCodeBatchPartial
	}
	response.Code = respCode
	api.writeKVResponse(w, statusCode, response)
}

// handleBatchDelete godoc
// @Summary 批量删除
// @Description 批量删除多个key
// @Tags KV
// @Accept  json
// @Produce  json
// @Param   keys  body  []string  true  "key数组"
// @Success 200 {object} BatchResponse
// @Failure 400 {object} ErrorResponse
// @Router /api/v1/keys [delete]
func (api *API) handleBatchDelete(w http.ResponseWriter, r *http.Request) {
	// Check if this node is the leader
	if !api.raftManager.IsLeader() {
		// Forward to leader
		if err := api.forwardToLeader(w, r); err != nil {
			api.log.Errorf("Failed to forward BATCH_DELETE request to leader: %v", err)
			api.writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("Failed to forward to leader: %v", err))
		}
		return
	}

	// Parse JSON request
	var req BatchDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.writeError(w, http.StatusBadRequest, "Invalid JSON request")
		return
	}

	if len(req.Keys) == 0 {
		api.writeError(w, http.StatusBadRequest, "keys array cannot be empty")
		return
	}

	var errors []string
	successCount := 0
	errorCount := 0

	// Apply all delete operations to Raft
	for _, key := range req.Keys {
		if key == "" {
			errors = append(errors, "key cannot be empty")
			errorCount++
			continue
		}

		err := api.raftManager.Delete(key, req.ErrorOnNotExists)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to delete %s: %v", key, err))
			errorCount++
		} else {
			successCount++
		}
	}

	// 创建KV操作响应
	header := api.getKVHeader()
	response := BatchResponse{
		Header:       header,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
		Errors:       errors,
	}

	statusCode := http.StatusOK
	respCode := ErrorCodeOK
	if errorCount > 0 {
		statusCode = http.StatusPartialContent
		respCode = ErrorCodeBatchPartial
	}
	response.Code = respCode
	api.writeKVResponse(w, statusCode, response)
}

// handleHealth handles health check requests
func (api *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	header := api.getKVHeader()
	
	// Check if Raft is healthy
	if !api.raftManager.IsHealthy() {
		api.writeKVError(w, http.StatusServiceUnavailable, "raft is not healthy")
		return
	}

	// Determine node role
	var role string
	if api.raftManager.IsLeader() {
		role = "leader"
	} else {
		role = "follower"
	}

	response := struct {
		Header KVHeader `json:"header"`
		Status string   `json:"status"`
		Role   string   `json:"role"`
	}{
		Header: header,
		Status: "healthy",
		Role:   role,
	}

	api.writeKVResponse(w, http.StatusOK, response)
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

// getNodeIDAsNumber converts node name to numeric ID
func (api *API) getNodeIDAsNumber() string {
	nodeName := api.config.GetNodeID()
	
	// 简单的转换规则：node1 -> 1, node2 -> 2, node3 -> 3
	if strings.HasPrefix(nodeName, "node") {
		if len(nodeName) > 4 {
			return nodeName[4:] // 提取数字部分
		}
	}
	
	// 如果不是标准格式，返回原始名称
	return nodeName
}

// getKVHeader creates a KV operation response header
func (api *API) getKVHeader() KVHeader {
	// 从配置中获取cluster_id和node_id
	clusterID := api.config.ClusterID
	nodeID := api.getNodeIDAsNumber() // 使用数字ID
	
	// 获取raft的当前term和index
	raftTerm := uint64(0)
	revision := uint64(0)
	
	// 这里可以从raft manager获取更多信息
	// 暂时使用默认值，后续可以扩展
	
	// 确保cluster_id不为空
	if clusterID == "" {
		clusterID = "dcache-cluster" // 默认值
	}
	
	return KVHeader{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Revision:  revision,
		RaftTerm:  raftTerm,
	}
}

// writeKVResponse writes a KV operation JSON response
func (api *API) writeKVResponse(w http.ResponseWriter, statusCode int, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

// writeKVErrorWithCode writes a standardized error response with a specific code
func (api *API) writeKVErrorWithCode(w http.ResponseWriter, statusCode int, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
		Code:  code,
	})
}

// 修改 writeKVError 使用细化的错误码
func (api *API) writeKVError(w http.ResponseWriter, statusCode int, message string) {
	var errorCode int
	switch statusCode {
	case http.StatusNotFound:
		errorCode = ErrorCodeNotFound
	case http.StatusBadRequest:
		errorCode = ErrorCodeInvalidArgument
	case http.StatusServiceUnavailable:
		errorCode = ErrorCodeNotLeader
	default:
		errorCode = ErrorCodeInternal
	}
	
	// 创建包含header的错误响应
	header := api.getKVHeader()
	errorResponse := struct {
		Header       KVHeader `json:"header"`
		SuccessCount int      `json:"success_count"`
		ErrorCount   int      `json:"error_count"`
		Code         int      `json:"code"`
		Error        string   `json:"error"`
	}{
		Header:       header,
		SuccessCount: 0,
		ErrorCount:   1,
		Code:         errorCode,
		Error:        message,
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errorResponse)
}

// writeError writes an error response
func (api *API) writeError(w http.ResponseWriter, statusCode int, message string) {
	api.writeResponse(w, statusCode, Response{
		Success: false,
		Error:   message,
	})
}

// handleGetMembers handles GET /api/v1/cluster/members requests
func (api *API) handleGetMembers(w http.ResponseWriter, r *http.Request) {
	header := api.getKVHeader()
	
	// Get cluster configuration
	conf := api.raftManager.GetConfiguration()
	var members []ClusterMember
	
	for _, server := range conf.Servers {
		// Determine role
		var role string
		if string(server.ID) == api.config.GetNodeID() && api.raftManager.IsLeader() {
			role = "leader"
		} else if string(server.ID) == api.config.GetNodeID() {
			role = "follower"
		} else {
			// For other nodes, we can't determine their exact role from this node
			// They could be leader or follower
			role = "unknown"
		}
		
		// Extract node ID from server ID
		nodeID := string(server.ID)
		if strings.HasPrefix(nodeID, "node") && len(nodeID) > 4 {
			nodeID = nodeID[4:] // Extract numeric part
		}
		
		member := ClusterMember{
			NodeID:   nodeID,
			NodeName: string(server.ID),
			Role:     role,
			Address:  string(server.Address),
		}
		members = append(members, member)
	}
	
	response := struct {
		Header  KVHeader        `json:"header"`
		Members []ClusterMember `json:"members"`
	}{
		Header:  header,
		Members: members,
	}
	
	api.writeKVResponse(w, http.StatusOK, response)
} 