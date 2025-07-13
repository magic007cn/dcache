package main

import (
	"fmt"
	"os"

	"dcache/internal/server"
	"dcache/internal/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	logLevel string
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
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().String("cluster-id", "", "cluster id")
	rootCmd.PersistentFlags().String("data-dir", "", "data directory")
	rootCmd.PersistentFlags().String("listen-client-urls", "", "listen client urls")
	rootCmd.PersistentFlags().String("advertise-client-urls", "", "advertise client urls")
	rootCmd.PersistentFlags().String("listen-peer-urls", "", "listen peer urls")
	rootCmd.PersistentFlags().String("initial-advertise-peer-urls", "", "initial advertise peer urls")
	rootCmd.PersistentFlags().String("initial-cluster", "", "initial cluster config")
	rootCmd.PersistentFlags().String("node-id", "", "node id")

	// 绑定flag到viper
	_ = viper.BindPFlag("log-level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("cluster-id", rootCmd.PersistentFlags().Lookup("cluster-id"))
	_ = viper.BindPFlag("data-dir", rootCmd.PersistentFlags().Lookup("data-dir"))
	_ = viper.BindPFlag("listen-client-urls", rootCmd.PersistentFlags().Lookup("listen-client-urls"))
	_ = viper.BindPFlag("advertise-client-urls", rootCmd.PersistentFlags().Lookup("advertise-client-urls"))
	_ = viper.BindPFlag("listen-peer-urls", rootCmd.PersistentFlags().Lookup("listen-peer-urls"))
	_ = viper.BindPFlag("initial-advertise-peer-urls", rootCmd.PersistentFlags().Lookup("initial-advertise-peer-urls"))
	_ = viper.BindPFlag("initial-cluster", rootCmd.PersistentFlags().Lookup("initial-cluster"))
	_ = viper.BindPFlag("node-id", rootCmd.PersistentFlags().Lookup("node-id"))

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

	return srv.Start()
} 