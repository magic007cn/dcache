package server

import (
	"context"
	"fmt"
	"strings"
	"sync"

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

// Set implements the Set RPC method
func (s *GrpcServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if req.Key == "" {
		return &pb.SetResponse{Success: false, Error: "key cannot be empty"}, nil
	}

	// Check if this node is the leader
	if !s.raftManager.IsLeader() {
		// Forward to leader
		return s.forwardSetToLeader(ctx, req)
	}

	// Apply to Raft
	err := s.raftManager.Apply([]byte(fmt.Sprintf("SET:%s:%s", req.Key, req.Value)))
	if err != nil {
		s.logger.Errorf("Failed to apply SET operation to Raft: %v", err)
		return &pb.SetResponse{Success: false, Error: err.Error()}, nil
	}

	return &pb.SetResponse{Success: true}, nil
}

// Get implements the Get RPC method
func (s *GrpcServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return &pb.GetResponse{Success: false, Error: "key cannot be empty"}, nil
	}

	// Check if this node is the leader
	if !s.raftManager.IsLeader() {
		// Forward to leader
		return s.forwardGetToLeader(ctx, req)
	}

	// Get from storage
	value, err := s.raftManager.Get(req.Key)
	if err != nil {
		s.logger.Errorf("Failed to get key %s: %v", req.Key, err)
		return &pb.GetResponse{Success: false, Error: err.Error()}, nil
	}

	if value == "" {
		return &pb.GetResponse{Success: false, Error: "key not found"}, nil
	}

	return &pb.GetResponse{Success: true, Value: value}, nil
}

// Delete implements the Delete RPC method
func (s *GrpcServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if req.Key == "" {
		return &pb.DeleteResponse{Success: false, Error: "key cannot be empty"}, nil
	}

	// Check if this node is the leader
	if !s.raftManager.IsLeader() {
		// Forward to leader
		return s.forwardDeleteToLeader(ctx, req)
	}

	// Apply to Raft
	err := s.raftManager.Apply([]byte(fmt.Sprintf("DELETE:%s", req.Key)))
	if err != nil {
		s.logger.Errorf("Failed to apply DELETE operation to Raft: %v", err)
		return &pb.DeleteResponse{Success: false, Error: err.Error()}, nil
	}

	return &pb.DeleteResponse{Success: true}, nil
}

// Scan implements the Scan RPC method
func (s *GrpcServer) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if req.Prefix == "" {
		return &pb.ScanResponse{Success: false, Error: "prefix cannot be empty"}, nil
	}

	// Check if this node is the leader
	if !s.raftManager.IsLeader() {
		// Forward to leader
		return s.forwardScanToLeader(ctx, req)
	}

	// Scan from storage
	pairs, err := s.raftManager.Scan(req.Prefix, int(req.Limit))
	if err != nil {
		s.logger.Errorf("Failed to scan with prefix %s: %v", req.Prefix, err)
		return &pb.ScanResponse{Success: false, Error: err.Error()}, nil
	}

	// Convert to protobuf format
	var keyValues []*pb.KeyValue
	for _, pair := range pairs {
		keyValues = append(keyValues, &pb.KeyValue{
			Key:   pair.Key,
			Value: pair.Value,
		})
	}

	return &pb.ScanResponse{Success: true, Pairs: keyValues}, nil
}

// Status implements the Status RPC method
func (s *GrpcServer) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	leader := s.raftManager.GetLeader()
	nodes := s.raftManager.GetNodes()

	return &pb.StatusResponse{
		Success: true,
		Leader:  leader,
		Nodes:   nodes,
	}, nil
}

// Health implements the Health RPC method
func (s *GrpcServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	// Check if Raft is healthy
	if !s.raftManager.IsHealthy() {
		return &pb.HealthResponse{Success: false, Status: "unhealthy"}, nil
	}

	return &pb.HealthResponse{Success: true, Status: "healthy"}, nil
}

// forwardSetToLeader forwards Set request to leader
func (s *GrpcServer) forwardSetToLeader(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.SetResponse{Success: false, Error: "no leader available"}, nil
	}

	// Convert Raft address to gRPC address
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	
	// Create gRPC connection to leader
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.SetResponse{Success: false, Error: fmt.Sprintf("failed to connect to leader: %v", err)}, nil
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewDCacheClient(conn)
	
	// Forward request
	resp, err := client.Set(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Set request to leader: %v", err)
		return &pb.SetResponse{Success: false, Error: fmt.Sprintf("failed to forward to leader: %v", err)}, nil
	}

	return resp, nil
}

// forwardGetToLeader forwards Get request to leader
func (s *GrpcServer) forwardGetToLeader(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.GetResponse{Success: false, Error: "no leader available"}, nil
	}

	// Convert Raft address to gRPC address
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	
	// Create gRPC connection to leader
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.GetResponse{Success: false, Error: fmt.Sprintf("failed to connect to leader: %v", err)}, nil
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewDCacheClient(conn)
	
	// Forward request
	resp, err := client.Get(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Get request to leader: %v", err)
		return &pb.GetResponse{Success: false, Error: fmt.Sprintf("failed to forward to leader: %v", err)}, nil
	}

	return resp, nil
}

// forwardDeleteToLeader forwards Delete request to leader
func (s *GrpcServer) forwardDeleteToLeader(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.DeleteResponse{Success: false, Error: "no leader available"}, nil
	}

	// Convert Raft address to gRPC address
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	
	// Create gRPC connection to leader
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.DeleteResponse{Success: false, Error: fmt.Sprintf("failed to connect to leader: %v", err)}, nil
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewDCacheClient(conn)
	
	// Forward request
	resp, err := client.Delete(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Delete request to leader: %v", err)
		return &pb.DeleteResponse{Success: false, Error: fmt.Sprintf("failed to forward to leader: %v", err)}, nil
	}

	return resp, nil
}

// forwardScanToLeader forwards Scan request to leader
func (s *GrpcServer) forwardScanToLeader(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	leader := s.raftManager.GetLeader()
	if leader == "" {
		return &pb.ScanResponse{Success: false, Error: "no leader available"}, nil
	}

	// Convert Raft address to gRPC address
	grpcAddr := s.convertRaftAddrToGrpcAddr(string(leader))
	
	// Create gRPC connection to leader
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		s.logger.Errorf("Failed to connect to leader gRPC: %v", err)
		return &pb.ScanResponse{Success: false, Error: fmt.Sprintf("failed to connect to leader: %v", err)}, nil
	}
	defer conn.Close()

	// Create gRPC client
	client := pb.NewDCacheClient(conn)
	
	// Forward request
	resp, err := client.Scan(ctx, req)
	if err != nil {
		s.logger.Errorf("Failed to forward Scan request to leader: %v", err)
		return &pb.ScanResponse{Success: false, Error: fmt.Sprintf("failed to forward to leader: %v", err)}, nil
	}

	return resp, nil
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