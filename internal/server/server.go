package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"

	"dcache/internal/config"
	"dcache/internal/raft"
	"path/filepath"
	"github.com/gofrs/flock"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc/reflection"
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
	lockFile    *flock.Flock
}

// New creates a new server instance
func New(cfg *config.Config) (*Server, error) {
	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// Data-dir lock: 防止多进程写同一目录（仅在 persistent 模式下）
	var lock *flock.Flock
	if cfg.DataDir != "" && cfg.StorageMode == "persistent" {
		lockPath := filepath.Join(cfg.DataDir, ".lock")
		lock = flock.New(lockPath)
		locked, err := lock.TryLock()
		if err != nil {
			return nil, fmt.Errorf("failed to lock data-dir: %v", err)
		}
		if !locked {
			return nil, fmt.Errorf("data-dir %s is already locked by another process", cfg.DataDir)
		}
		log.Infof("Locked data-dir: %s", cfg.DataDir)
	}

	// Create Raft manager
	raftManager, err := raft.NewManager(cfg, log)
	if err != nil {
		if lock != nil {
			_ = lock.Unlock()
		}
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
		lockFile:    lock,
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
	
	// 启用 gRPC 反射服务
	reflection.Register(s.grpcServer2)
	
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

	return mux.Serve()
}

// Stop stops the server gracefully with timeout
func (s *Server) Stop() error {
	return s.StopWithContext(context.Background())
}

// StopWithContext stops the server gracefully with context timeout
func (s *Server) StopWithContext(ctx context.Context) error {
	s.log.Info("Stopping server gracefully...")
	
	// Create error channel to collect shutdown errors
	errChan := make(chan error, 3)
	
	// Stop HTTP server
	if s.httpServer != nil {
		go func() {
			if err := s.httpServer.Shutdown(ctx); err != nil {
				s.log.Errorf("Failed to shutdown HTTP server: %v", err)
				errChan <- fmt.Errorf("HTTP server shutdown error: %v", err)
			} else {
				s.log.Info("HTTP server stopped gracefully")
				errChan <- nil
			}
		}()
	} else {
		errChan <- nil
	}
	
	// Stop gRPC server
	if s.grpcServer2 != nil {
		go func() {
			// Create a channel for gRPC graceful stop
			grpcDone := make(chan struct{})
			go func() {
				s.grpcServer2.GracefulStop()
				close(grpcDone)
			}()
			
			// Wait for gRPC to stop or context timeout
			select {
			case <-grpcDone:
				s.log.Info("gRPC server stopped gracefully")
				errChan <- nil
			case <-ctx.Done():
				s.log.Warn("gRPC graceful stop timeout, forcing stop")
				s.grpcServer2.Stop()
				errChan <- fmt.Errorf("gRPC server stop timeout")
			}
		}()
	} else {
		errChan <- nil
	}
	
	// Stop Raft manager
	if s.raftManager != nil {
		go func() {
			if err := s.raftManager.Stop(); err != nil {
				s.log.Errorf("Failed to stop raft manager: %v", err)
				errChan <- fmt.Errorf("Raft manager stop error: %v", err)
			} else {
				s.log.Info("Raft manager stopped gracefully")
				errChan <- nil
			}
		}()
	} else {
		errChan <- nil
	}
	
	// Wait for all components to stop or context timeout
	var errors []error
	for i := 0; i < 3; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				errors = append(errors, err)
			}
		case <-ctx.Done():
			s.log.Warn("Shutdown timeout, some components may not have stopped gracefully")
			return fmt.Errorf("shutdown timeout: %v", ctx.Err())
		}
	}
	
	if len(errors) > 0 {
		s.log.Errorf("Server stopped with errors: %v", errors)
		return fmt.Errorf("shutdown errors: %v", errors)
	}
	
	s.log.Info("Server stopped gracefully")
	
	// 释放 data-dir lock
	if s.lockFile != nil {
		_ = s.lockFile.Unlock()
		s.log.Info("Released data-dir lock")
	}
	return nil
}

func (s *Server) GetRaftManager() *raft.Manager {
	return s.raftManager
}

func (s *Server) GetAPI() *API {
	return s.api
} 