package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dcache/internal/server"
	"dcache/internal/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	logLevel string
	raftLogLevel string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "dcache",
		Short: "Distributed cache server using Raft consensus",
		Long: `A distributed in-memory cache server that uses Raft consensus protocol
for data consistency across multiple nodes.`,
		RunE: runServer,
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "warn", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&raftLogLevel, "raft-log-level", "warn", "raft log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().String("cluster-id", "", "cluster id")
	rootCmd.PersistentFlags().String("data-dir", "", "data directory")
	rootCmd.PersistentFlags().String("listen-client-urls", "", "listen client urls")
	rootCmd.PersistentFlags().String("advertise-client-urls", "", "advertise client urls")
	rootCmd.PersistentFlags().String("listen-peer-urls", "", "listen peer urls")
	rootCmd.PersistentFlags().String("initial-advertise-peer-urls", "", "initial advertise peer urls")
	rootCmd.PersistentFlags().String("initial-cluster", "", "initial cluster config")
	rootCmd.PersistentFlags().String("node-id", "", "node id")
	rootCmd.PersistentFlags().String("storage-mode", "", "storage mode (inmemory, persistent)")

	// 绑定flag到viper
	_ = viper.BindPFlag("log-level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("raft-log-level", rootCmd.PersistentFlags().Lookup("raft-log-level"))
	_ = viper.BindPFlag("cluster-id", rootCmd.PersistentFlags().Lookup("cluster-id"))
	_ = viper.BindPFlag("data-dir", rootCmd.PersistentFlags().Lookup("data-dir"))
	_ = viper.BindPFlag("listen-client-urls", rootCmd.PersistentFlags().Lookup("listen-client-urls"))
	_ = viper.BindPFlag("advertise-client-urls", rootCmd.PersistentFlags().Lookup("advertise-client-urls"))
	_ = viper.BindPFlag("listen-peer-urls", rootCmd.PersistentFlags().Lookup("listen-peer-urls"))
	_ = viper.BindPFlag("initial-advertise-peer-urls", rootCmd.PersistentFlags().Lookup("initial-advertise-peer-urls"))
	_ = viper.BindPFlag("initial-cluster", rootCmd.PersistentFlags().Lookup("initial-cluster"))
	_ = viper.BindPFlag("node-id", rootCmd.PersistentFlags().Lookup("node-id"))
	_ = viper.BindPFlag("storage-mode", rootCmd.PersistentFlags().Lookup("storage-mode"))

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) error {
	// Set log level
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level: %v", err)
	}
	logrus.SetLevel(level)

	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	// Create and start server
	srv, err := server.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %v", err)
	}

	// Create context for graceful shutdown
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for either signal or server error
	select {
	case sig := <-sigChan:
		logrus.Infof("Received signal %v, starting graceful shutdown...", sig)
		
		// Create shutdown context with timeout
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		
		// Stop server gracefully
		if err := srv.StopWithContext(shutdownCtx); err != nil {
			logrus.Errorf("Error during graceful shutdown: %v", err)
			return err
		}
		
		logrus.Info("Server stopped gracefully")
		return nil
		
	case err := <-errChan:
		return fmt.Errorf("server error: %v", err)
	}
} 