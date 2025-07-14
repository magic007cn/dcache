package server

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"dcache/internal/config"
	"dcache/internal/storage"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	pb "dcache/proto"
)

// GrpcServer implements the DCache gRPC service
type GrpcServer struct {
	pb.UnimplementedDCacheServer
	raftManager RaftManager
	logger      *logrus.Logger
	mu          sync.RWMutex
}

// NewGrpcServer creates a new gRPC server
func NewGrpcServer(raftManager RaftManager) *GrpcServer {
	return &GrpcServer{
		raftManager: raftManager,
		logger:      logrus.New(),
	}
}

func (s *GrpcServer) buildHeader() *pb.Header {
	cfg := s.raftManager.GetConfig().(*config.Config)
	clusterID := cfg.ClusterID
	nodeName := cfg.GetNodeID()
	nodeID := nodeName
	if strings.HasPrefix(nodeName, "node") && len(nodeName) > 4 {
		nodeID = nodeName[4:]
	}
	return &pb.Header{
		ClusterId: clusterID,
		NodeId:    nodeID,
		Revision:  0,
		RaftTerm:  0,
	}
}

const (
	CodeOK              = int32(0)
	CodeKeyNotFound     = int32(5)
	CodeInvalidRequest  = int32(3)
	CodeInternalError   = int32(13)
	CodeNotLeader       = int32(9)
)

// Set implements the Set RPC method
func (s *GrpcServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	header := s.buildHeader()
	if req.Key == "" {
		return &pb.SetResponse{Header: header, Code: CodeInvalidRequest, Error: "key cannot be empty"}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardSetToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	err := s.raftManager.Apply([]byte(fmt.Sprintf("SET:%s:%s", req.Key, req.Value)))
	if err != nil {
		s.logger.Errorf("Failed to apply SET operation to Raft: %v", err)
		return &pb.SetResponse{Header: header, Code: CodeInternalError, Error: err.Error(), ErrorCount: 1}, nil
	}
	return &pb.SetResponse{Header: header, Code: CodeOK, SuccessCount: 1, ErrorCount: 0}, nil
}

func (s *GrpcServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	header := s.buildHeader()
	if req.Key == "" {
		return &pb.GetResponse{Header: header, Code: CodeInvalidRequest, Error: "key cannot be empty"}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardGetToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	value, err := s.raftManager.Get(req.Key)
	if err != nil {
		s.logger.Errorf("Failed to get key %s: %v", req.Key, err)
		return &pb.GetResponse{Header: header, Code: CodeInternalError, Error: err.Error()}, nil
	}
	if value == "" {
		return &pb.GetResponse{Header: header, Code: CodeKeyNotFound, Error: "key not found", SuccessCount: 0, ErrorCount: 1}, nil
	}
	return &pb.GetResponse{
		Header:       header,
		Kvs:          []*pb.KeyValue{{Key: req.Key, Value: value}},
		SuccessCount: 1,
		ErrorCount:   0,
		Code:         CodeOK,
	}, nil
}

func (s *GrpcServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	header := s.buildHeader()
	if req.Key == "" {
		return &pb.DeleteResponse{Header: header, Code: CodeInvalidRequest, Error: "key cannot be empty"}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardDeleteToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	// Use the same method as HTTP API to ensure consistency
	err := s.raftManager.Delete(req.Key, req.ErrorOnNotExists)
	if err != nil {
		s.logger.Errorf("Failed to apply DELETE operation to Raft: %v", err)
		return &pb.DeleteResponse{Header: header, Code: CodeInternalError, Error: err.Error(), ErrorCount: 1}, nil
	}
	return &pb.DeleteResponse{Header: header, SuccessCount: 1, ErrorCount: 0, Code: CodeOK}, nil
}

func (s *GrpcServer) RangeScan(ctx context.Context, req *pb.RangeScanRequest) (*pb.RangeScanResponse, error) {
	header := s.buildHeader()
	if req.Prefix == "" {
		return &pb.RangeScanResponse{Header: header, Code: CodeInvalidRequest, Error: "prefix cannot be empty"}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardRangeScanToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	pairs, err := s.raftManager.Scan(req.Prefix, int(req.Limit))
	if err != nil {
		s.logger.Errorf("Failed to range scan with prefix %s: %v", req.Prefix, err)
		return &pb.RangeScanResponse{Header: header, Code: CodeInternalError, Error: err.Error()}, nil
	}
	var keyValues []*pb.KeyValue
	for _, pair := range pairs {
		keyValues = append(keyValues, &pb.KeyValue{
			Key:   pair.Key,
			Value: pair.Value,
		})
	}
	return &pb.RangeScanResponse{Header: header, Kvs: keyValues, SuccessCount: int32(len(keyValues)), ErrorCount: 0, Code: CodeOK}, nil
}

// Status implements the Status RPC method
func (s *GrpcServer) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	nodes := s.raftManager.GetNodes()
	isLeader := s.raftManager.IsLeader()

	return &pb.StatusResponse{
		Header:   header,
		Leader:   leader,
		Nodes:    nodes,
		IsLeader: isLeader,
		Code:     CodeOK,
	}, nil
}

// Health implements the Health RPC method
func (s *GrpcServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	header := s.buildHeader()
	
	// Check if Raft is healthy
	if !s.raftManager.IsHealthy() {
		return &pb.HealthResponse{
			Header: header,
			Status: "unhealthy",
			Code:   CodeInternalError,
			Error:  "raft is not healthy",
		}, nil
	}

	// Determine node role
	var role string
	if s.raftManager.IsLeader() {
		role = "leader"
	} else {
		role = "follower"
	}

	return &pb.HealthResponse{
		Header: header,
		Status: "healthy",
		Role:   role,
		Code:   CodeOK,
	}, nil
}

// Members implements the Members RPC method
func (s *GrpcServer) Members(ctx context.Context, req *pb.MembersRequest) (*pb.MembersResponse, error) {
	header := s.buildHeader()
	
	// Get cluster configuration
	conf := s.raftManager.GetConfiguration()
	var members []*pb.ClusterMember
	
	for _, server := range conf.Servers {
		// Determine role
		var role string
		if string(server.ID) == s.raftManager.GetConfig().(*config.Config).GetNodeID() && s.raftManager.IsLeader() {
			role = "leader"
		} else if string(server.ID) == s.raftManager.GetConfig().(*config.Config).GetNodeID() {
			role = "follower"
		} else {
			// For other nodes, we can't determine their exact role from this node
			role = "unknown"
		}
		
		// Extract node ID from server ID
		nodeID := string(server.ID)
		if strings.HasPrefix(nodeID, "node") && len(nodeID) > 4 {
			nodeID = nodeID[4:] // Extract numeric part
		}
		
		member := &pb.ClusterMember{
			NodeId:   nodeID,
			NodeName: string(server.ID),
			Role:     role,
			Address:  string(server.Address),
		}
		members = append(members, member)
	}
	
	return &pb.MembersResponse{
		Header:  header,
		Members: members,
		Code:    CodeOK,
	}, nil
}

// BatchSet implements the BatchSet RPC method
func (s *GrpcServer) BatchSet(ctx context.Context, req *pb.BatchSetRequest) (*pb.BatchSetResponse, error) {
	header := s.buildHeader()
	if len(req.Pairs) == 0 {
		return &pb.BatchSetResponse{Header: header, Code: CodeInvalidRequest, Error: "pairs array cannot be empty", ErrorCount: 1}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardBatchSetToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	// 类型转换
	pairs := make([]storage.KeyValue, len(req.Pairs))
	for i, kv := range req.Pairs {
		pairs[i] = storage.KeyValue{Key: kv.Key, Value: kv.Value}
	}
	err := s.raftManager.BatchSet(pairs)
	successCount := 0
	errorCount := 0
	var errors []string
	if err != nil {
		errors = append(errors, err.Error())
		errorCount = len(req.Pairs)
	} else {
		successCount = len(req.Pairs)
	}
	code := CodeOK
	if errorCount > 0 {
		code = CodeInternalError
	}
	return &pb.BatchSetResponse{
		Header: header,
		SuccessCount: int32(successCount),
		ErrorCount: int32(errorCount),
		Errors: errors,
		Code: code,
	}, nil
}

// BatchGet implements the BatchGet RPC method
func (s *GrpcServer) BatchGet(ctx context.Context, req *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	header := s.buildHeader()
	if len(req.Keys) == 0 {
		return &pb.BatchGetResponse{Header: header, Code: CodeInvalidRequest, Error: "keys array cannot be empty", SuccessCount: 0, ErrorCount: 1}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardBatchGetToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	var kvs []*pb.KeyValue
	var errors []string
	successCount := 0
	errorCount := 0
	for _, key := range req.Keys {
		if key == "" {
			errors = append(errors, "key cannot be empty")
			errorCount++
			continue
		}
		value, err := s.raftManager.Get(key)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get %s: %v", key, err))
			errorCount++
		} else if value == "" {
			errors = append(errors, fmt.Sprintf("key %s not found", key))
			errorCount++
		} else {
			kvs = append(kvs, &pb.KeyValue{Key: key, Value: value})
			successCount++
		}
	}
	code := CodeOK
	if errorCount > 0 {
		code = CodeInternalError
	}
	return &pb.BatchGetResponse{
		Header:       header,
		Kvs:          kvs,
		SuccessCount: int32(successCount),
		ErrorCount:   int32(errorCount),
		Errors:       errors,
		Code:         code,
	}, nil
}

// BatchDelete implements the BatchDelete RPC method
func (s *GrpcServer) BatchDelete(ctx context.Context, req *pb.BatchDeleteRequest) (*pb.BatchDeleteResponse, error) {
	header := s.buildHeader()
	if len(req.Keys) == 0 {
		return &pb.BatchDeleteResponse{Header: header, Code: CodeInvalidRequest, Error: "keys array cannot be empty", ErrorCount: 1}, nil
	}
	if !s.raftManager.IsLeader() {
		resp, _ := s.forwardBatchDeleteToLeader(ctx, req)
		if resp != nil {
			resp.Header = header
		}
		return resp, nil
	}
	var errors []string
	successCount := 0
	errorCount := 0
	for _, key := range req.Keys {
		if key == "" {
			errors = append(errors, "key cannot be empty")
			errorCount++
			continue
		}
		// Use the same method as HTTP API to ensure consistency
		err := s.raftManager.Delete(key, req.ErrorOnNotExists)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to delete %s: %v", key, err))
			errorCount++
		} else {
			successCount++
		}
	}
	code := CodeOK
	if errorCount > 0 {
		code = CodeInternalError
	}
	return &pb.BatchDeleteResponse{
		Header: header,
		SuccessCount: int32(successCount),
		ErrorCount: int32(errorCount),
		Errors: errors,
		Code: code,
	}, nil
}

// StreamSet implements the StreamSet RPC method
func (s *GrpcServer) StreamSet(stream pb.DCache_StreamSetServer) error {
	header := s.buildHeader()
	var successCount, errorCount int32
	var errors []string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			code := CodeOK
			if errorCount > 0 {
				code = CodeInternalError
			}
			return stream.SendAndClose(&pb.StreamSetResponse{
				Header: header,
				SuccessCount: successCount,
				ErrorCount: errorCount,
				Errors: errors,
				Code: code,
			})
		}
		if err != nil {
			return err
		}
		if req.Key == "" {
			errors = append(errors, "key cannot be empty")
			errorCount++
			continue
		}
		if !s.raftManager.IsLeader() {
			errors = append(errors, "not leader")
			errorCount++
			continue
		}
		applyErr := s.raftManager.Apply([]byte(fmt.Sprintf("SET:%s:%s", req.Key, req.Value)))
		if applyErr != nil {
			errors = append(errors, fmt.Sprintf("failed to set %s: %v", req.Key, applyErr))
			errorCount++
		} else {
			successCount++
		}
	}
}

// forwardSetToLeader forwards Set request to leader
func (s *GrpcServer) forwardSetToLeader(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.SetResponse{Header: header, Code: CodeNotLeader, Error: "no leader available"}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.SetResponse{Header: header, Code: CodeInternalError, Error: "failed to connect to leader"}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	resp, err := client.Set(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Set request to leader: %v", err)
		return &pb.SetResponse{Header: header, Code: CodeInternalError, Error: "failed to forward to leader"}, nil
	}
	resp.Header = header
	return resp, nil
}

// forwardGetToLeader forwards Get request to leader
func (s *GrpcServer) forwardGetToLeader(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.GetResponse{Header: header, Code: CodeNotLeader, Error: "no leader available"}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.GetResponse{Header: header, Code: CodeInternalError, Error: "failed to connect to leader"}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	resp, err := client.Get(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Get request to leader: %v", err)
		return &pb.GetResponse{Header: header, Code: CodeInternalError, Error: "failed to forward to leader"}, nil
	}
	resp.Header = header
	return resp, nil
}

// forwardDeleteToLeader forwards Delete request to leader
func (s *GrpcServer) forwardDeleteToLeader(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.DeleteResponse{Header: header, Code: CodeNotLeader, Error: "no leader available"}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.DeleteResponse{Header: header, Code: CodeInternalError, Error: "failed to connect to leader"}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	resp, err := client.Delete(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Delete request to leader: %v", err)
		return &pb.DeleteResponse{Header: header, Code: CodeInternalError, Error: "failed to forward to leader"}, nil
	}
	resp.Header = header
	return resp, nil
}

// forwardRangeScanToLeader forwards RangeScan request to leader
func (s *GrpcServer) forwardRangeScanToLeader(ctx context.Context, req *pb.RangeScanRequest) (*pb.RangeScanResponse, error) {
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.RangeScanResponse{Code: CodeNotLeader, Error: "no leader available"}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(leader)
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader %s: %v", grpcAddr, err)
		return &pb.RangeScanResponse{Code: CodeInternalError, Error: fmt.Sprintf("failed to connect to leader: %v", err)}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	return client.RangeScan(ctx, req)
}

// convertRaftAddrToGrpcAddr converts Raft address to gRPC address
func (s *GrpcServer) convertRaftAddrToGrpcAddr(raftAddr string) string {
	// Convert Raft address (e.g., 127.0.0.1:9091) to gRPC address (127.0.0.1:8081)
	// gRPC and HTTP share the same port using cmux
	parts := strings.Split(raftAddr, ":")
	if len(parts) != 2 {
		return "127.0.0.1:8081" // Default
	}
	
	// Determine HTTP/gRPC port based on Raft port
	var httpGrpcPort string
	switch {
	case strings.Contains(raftAddr, "9091"):
		httpGrpcPort = "8081"
	case strings.Contains(raftAddr, "9092"):
		httpGrpcPort = "8082"
	case strings.Contains(raftAddr, "9093"):
		httpGrpcPort = "8083"
	default:
		httpGrpcPort = "8081" // Default
	}
	
	return fmt.Sprintf("%s:%s", parts[0], httpGrpcPort)
}

// Register registers the gRPC server with the given server
func (s *GrpcServer) Register(server *grpc.Server) {
	pb.RegisterDCacheServer(server, s)
}

// forwardBatchSetToLeader forwards BatchSet request to leader
func (s *GrpcServer) forwardBatchSetToLeader(ctx context.Context, req *pb.BatchSetRequest) (*pb.BatchSetResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.BatchSetResponse{Header: header, Code: CodeNotLeader, Error: "no leader available", ErrorCount: 1}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.BatchSetResponse{Header: header, Code: CodeInternalError, Error: "failed to connect to leader", ErrorCount: 1}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	resp, err := client.BatchSet(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward BatchSet request to leader: %v", err)
		return &pb.BatchSetResponse{Header: header, Code: CodeInternalError, Error: "failed to forward to leader", ErrorCount: 1}, nil
	}
	resp.Header = header
	return resp, nil
}

// forwardBatchGetToLeader forwards BatchGet request to leader
func (s *GrpcServer) forwardBatchGetToLeader(ctx context.Context, req *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.BatchGetResponse{Header: header, Code: CodeNotLeader, Error: "no leader available", SuccessCount: 0, ErrorCount: 1}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.BatchGetResponse{Header: header, Code: CodeInternalError, Error: "failed to connect to leader", SuccessCount: 0, ErrorCount: 1}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	resp, err := client.BatchGet(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward BatchGet request to leader: %v", err)
		return &pb.BatchGetResponse{Header: header, Code: CodeInternalError, Error: "failed to forward to leader", SuccessCount: 0, ErrorCount: 1}, nil
	}
	resp.Header = header
	return resp, nil
}

// forwardBatchDeleteToLeader forwards BatchDelete request to leader
func (s *GrpcServer) forwardBatchDeleteToLeader(ctx context.Context, req *pb.BatchDeleteRequest) (*pb.BatchDeleteResponse, error) {
	header := s.buildHeader()
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.BatchDeleteResponse{Header: header, Code: CodeNotLeader, Error: "no leader available", ErrorCount: 1}, nil
	}
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.BatchDeleteResponse{Header: header, Code: CodeInternalError, Error: "failed to connect to leader", ErrorCount: 1}, nil
	}
	defer conn.Close()
	client := pb.NewDCacheClient(conn)
	resp, err := client.BatchDelete(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward BatchDelete request to leader: %v", err)
		return &pb.BatchDeleteResponse{Header: header, Code: CodeInternalError, Error: "failed to forward to leader", ErrorCount: 1}, nil
	}
	resp.Header = header
	return resp, nil
} 