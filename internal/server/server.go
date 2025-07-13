package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"dcache/internal/config"
	"dcache/internal/raft"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"github.com/soheilhy/cmux"
)

// Server represents the main server
type Server struct {
	config      *config.Config
	raftManager *raft.Manager
	api         *API
	grpcServer  *GrpcServer
	log         *logrus.Logger
	httpServer  *http.Server
	grpcServer2 *grpc.Server
}

// New creates a new server instance
func New(cfg *config.Config) (*Server, error) {
	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// Create Raft manager
	raftManager, err := raft.NewManager(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft manager: %v", err)
	}

	// Create API server
	api := NewAPI(raftManager, cfg, log)

	// Create gRPC server
	grpcServer := NewGrpcServer(raftManager)

	return &Server{
		config:      cfg,
		raftManager: raftManager,
		api:         api,
		grpcServer:  grpcServer,
		log:         log,
	}, nil
}

// Start starts the server with cmux for HTTP/gRPC port multiplexing
func (s *Server) Start() error {
	if err := s.raftManager.Start(); err != nil {
		return fmt.Errorf("failed to start raft manager: %v", err)
	}

	clientAddr := strings.TrimPrefix(s.config.ListenClientURLs, "http://")
	if !strings.Contains(clientAddr, ":") {
		clientAddr = clientAddr + ":8080"
	}

	// 1. 创建 net.Listener
	ln, err := net.Listen("tcp", clientAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", clientAddr, err)
	}
	mux := cmux.New(ln)

	// gRPC 匹配规则
	grpcL := mux.MatchWithWriters(
	 cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"),
	)
	// otherwise serve http
	httpL := mux.Match(cmux.Any())
   

	// 2. 创建 gRPC server
	s.grpcServer2 = grpc.NewServer()
	s.grpcServer.Register(s.grpcServer2)
	
	// 确保 gRPC server 支持 HTTP/2
	s.log.Infof("gRPC server created with HTTP/2 support")
	
	// 3. 创建 HTTP mux
	hmux := http.NewServeMux()
	hmux.Handle("/api/", s.api.Router)
	hmux.Handle("/health", s.api.Router)

	// 5. 启动 gRPC server
	go func() {
		s.log.Infof("Starting gRPC server on %s", clientAddr)
		s.log.Infof("gRPC listener address: %s", grpcL.Addr().String())
		s.log.Infof("gRPC server type: %T", s.grpcServer2)
		
		// 启动 gRPC server
		if err := s.grpcServer2.Serve(grpcL); err != nil {
			s.log.Errorf("gRPC server exited with error: %v", err)
		} else {
			s.log.Infof("gRPC server stopped normally")
		}
	}()
	
	// 等待一小段时间让 gRPC server 启动
	time.Sleep(100 * time.Millisecond)
	s.log.Infof("gRPC server should be ready on %s", clientAddr)
	
	// 测试 gRPC server 是否正在监听
	s.log.Infof("Testing gRPC server readiness...")

	// 6. 启动 HTTP server
	s.httpServer = &http.Server{
		Handler: hmux,
	}
	go func() {
		s.log.Infof("Starting HTTP server on %s", clientAddr)
		if err := s.httpServer.Serve(httpL); err != nil && err != http.ErrServerClosed {
			s.log.Errorf("HTTP server exited: %v", err)
		}
	}()

	s.log.Infof("Starting cmux on %s", clientAddr)
	return mux.Serve()
}

// Stop stops the server
func (s *Server) Stop() error {
	s.log.Info("Stopping server...")
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(context.Background()); err != nil {
			s.log.Errorf("Failed to shutdown HTTP server: %v", err)
		}
	}
	if s.grpcServer2 != nil {
		s.grpcServer2.GracefulStop()
	}
	if s.raftManager != nil {
		if err := s.raftManager.Stop(); err != nil {
			s.log.Errorf("Failed to stop raft manager: %v", err)
		}
	}
	s.log.Info("Server stopped")
	return nil
}

func (s *Server) GetRaftManager() *raft.Manager {
	return s.raftManager
}

func (s *Server) GetAPI() *API {
	return s.api
} 