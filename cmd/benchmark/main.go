package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	pb "dcache/proto"
)

// BenchmarkConfig represents benchmark configuration
type BenchmarkConfig struct {
	ServerAddr    string
	GrpcAddr      string // 新增
	UseGrpc       bool   // 新增
	Concurrency   int
	Duration      time.Duration
	KeyPrefix     string
	ValueSize     int
	ScanPrefix    string
	ScanLimit     int
	ReportInterval time.Duration
}

// BenchmarkResult represents benchmark result
type BenchmarkResult struct {
	Operation     string
	TotalOps      int64
	TotalTime     time.Duration
	Throughput    float64 // ops/sec
	AvgLatency    time.Duration
	MinLatency    time.Duration
	MaxLatency    time.Duration
	P50Latency    time.Duration
	P95Latency    time.Duration
	P99Latency    time.Duration
	Errors        int64
	ErrorRate     float64
}

// LatencyStats tracks latency statistics
type LatencyStats struct {
	latencies []time.Duration
	mu        sync.Mutex
}

func (ls *LatencyStats) Add(latency time.Duration) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.latencies = append(ls.latencies, latency)
}

func (ls *LatencyStats) Calculate() (min, max, avg, p50, p95, p99 time.Duration) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	
	if len(ls.latencies) == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	// Sort latencies
	latencies := make([]time.Duration, len(ls.latencies))
	copy(latencies, ls.latencies)
	
	min = latencies[0]
	max = latencies[0]
	total := time.Duration(0)
	
	for _, l := range latencies {
		if l < min {
			min = l
		}
		if l > max {
			max = l
		}
		total += l
	}
	
	avg = total / time.Duration(len(latencies))
	
	// Calculate percentiles
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p95 = latencies[len(latencies)*95/100]
		p99 = latencies[len(latencies)*99/100]
	}
	
	return
}

// BenchmarkClient represents a benchmark client
type BenchmarkClient struct {
	config     *BenchmarkConfig
	// HTTP
	httpClient *http.Client
	// gRPC
	grpcConn   *grpc.ClientConn
	grpcClient pb.DCacheClient
	logger     *logrus.Logger
}

func NewBenchmarkClient(config *BenchmarkConfig) *BenchmarkClient {
	bc := &BenchmarkClient{
		config: config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logrus.New(),
	}
	if config.UseGrpc && config.GrpcAddr != "" {
		conn, err := grpc.Dial(config.GrpcAddr, grpc.WithInsecure())
		if err != nil {
			panic(fmt.Sprintf("failed to connect to gRPC server: %v", err))
		}
		bc.grpcConn = conn
		bc.grpcClient = pb.NewDCacheClient(conn)
	}
	return bc
}

// SetRequest represents a set operation request
type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Response represents API response
type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// benchmarkSet performs SET operation benchmark
func (bc *BenchmarkClient) benchmarkSet(wg *sync.WaitGroup, resultChan chan<- *BenchmarkResult) {
	defer wg.Done()
	stats := &LatencyStats{}
	var totalOps int64
	var errors int64
	startTime := time.Now()
	value := generateValue(bc.config.ValueSize)
	ctx := context.Background()
	for time.Since(startTime) < bc.config.Duration {
		key := fmt.Sprintf("%s_%d", bc.config.KeyPrefix, totalOps)
		if bc.config.UseGrpc && bc.grpcClient != nil {
			// gRPC
			start := time.Now()
			_, err := bc.grpcClient.Set(ctx, &pb.SetRequest{Key: key, Value: value})
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			stats.Add(latency)
			totalOps++
		} else {
			// HTTP
			reqBody := SetRequest{Key: key, Value: value}
			jsonData, _ := json.Marshal(reqBody)
			start := time.Now()
			resp, err := bc.httpClient.Post(
				fmt.Sprintf("http://%s/api/v1/set", bc.config.ServerAddr),
				"application/json",
				bytes.NewBuffer(jsonData),
			)
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.StatusCode != http.StatusOK {
				errors++
				resp.Body.Close()
				continue
			}
			var apiResp Response
			if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
				errors++
				resp.Body.Close()
				continue
			}
			resp.Body.Close()
			if !apiResp.Success {
				errors++
				continue
			}
			stats.Add(latency)
			totalOps++
		}
	}
	// Calculate statistics
	min, max, avg, p50, p95, p99 := stats.Calculate()
	
	result := &BenchmarkResult{
		Operation:     "SET",
		TotalOps:      totalOps,
		TotalTime:     time.Since(startTime),
		Throughput:    float64(totalOps) / time.Since(startTime).Seconds(),
		AvgLatency:    avg,
		MinLatency:    min,
		MaxLatency:    max,
		P50Latency:    p50,
		P95Latency:    p95,
		P99Latency:    p99,
		Errors:        errors,
		ErrorRate:     float64(errors) / float64(totalOps+errors) * 100,
	}
	
	resultChan <- result
}

// benchmarkGet performs GET operation benchmark
func (bc *BenchmarkClient) benchmarkGet(wg *sync.WaitGroup, resultChan chan<- *BenchmarkResult) {
	defer wg.Done()
	stats := &LatencyStats{}
	var totalOps int64
	var errors int64
	// 预置数据
	value := generateValue(bc.config.ValueSize)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%s_get_%d", bc.config.KeyPrefix, i)
		if bc.config.UseGrpc && bc.grpcClient != nil {
			_, _ = bc.grpcClient.Set(ctx, &pb.SetRequest{Key: key, Value: value})
		} else {
			reqBody := SetRequest{Key: key, Value: value}
			jsonData, _ := json.Marshal(reqBody)
			resp, err := bc.httpClient.Post(
				fmt.Sprintf("http://%s/api/v1/set", bc.config.ServerAddr),
				"application/json",
				bytes.NewBuffer(jsonData),
			)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
			}
		}
	}
	startTime := time.Now()
	for time.Since(startTime) < bc.config.Duration {
		key := fmt.Sprintf("%s_get_%d", bc.config.KeyPrefix, totalOps%1000)
		if bc.config.UseGrpc && bc.grpcClient != nil {
			start := time.Now()
			_, err := bc.grpcClient.Get(ctx, &pb.GetRequest{Key: key})
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			stats.Add(latency)
			totalOps++
		} else {
			start := time.Now()
			resp, err := bc.httpClient.Get(
				fmt.Sprintf("http://%s/api/v1/get/%s", bc.config.ServerAddr, key),
			)
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.StatusCode != http.StatusOK {
				errors++
				resp.Body.Close()
				continue
			}
			var apiResp Response
			if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
				errors++
				resp.Body.Close()
				continue
			}
			resp.Body.Close()
			if !apiResp.Success {
				errors++
				continue
			}
			stats.Add(latency)
			totalOps++
		}
	}
	// Calculate statistics
	min, max, avg, p50, p95, p99 := stats.Calculate()
	
	result := &BenchmarkResult{
		Operation:     "GET",
		TotalOps:      totalOps,
		TotalTime:     time.Since(startTime),
		Throughput:    float64(totalOps) / time.Since(startTime).Seconds(),
		AvgLatency:    avg,
		MinLatency:    min,
		MaxLatency:    max,
		P50Latency:    p50,
		P95Latency:    p95,
		P99Latency:    p99,
		Errors:        errors,
		ErrorRate:     float64(errors) / float64(totalOps+errors) * 100,
	}
	
	resultChan <- result
}

// benchmarkScan performs SCAN operation benchmark
func (bc *BenchmarkClient) benchmarkScan(wg *sync.WaitGroup, resultChan chan<- *BenchmarkResult) {
	defer wg.Done()
	stats := &LatencyStats{}
	var totalOps int64
	var errors int64
	
	// First, set some data for SCAN testing (before timing)
	value := generateValue(bc.config.ValueSize)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%s_scan_%d", bc.config.KeyPrefix, i)
		if bc.config.UseGrpc && bc.grpcClient != nil {
			_, _ = bc.grpcClient.Set(context.Background(), &pb.SetRequest{Key: key, Value: value})
		} else {
			reqBody := SetRequest{
				Key:   key,
				Value: value,
			}
			
			jsonData, _ := json.Marshal(reqBody)
			resp, err := bc.httpClient.Post(
				fmt.Sprintf("http://%s/api/v1/set", bc.config.ServerAddr),
				"application/json",
				bytes.NewBuffer(jsonData),
			)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
			}
		}
	}
	
	// Now benchmark SCAN operations
	startTime := time.Now()
	for time.Since(startTime) < bc.config.Duration {
		start := time.Now()
		if bc.config.UseGrpc && bc.grpcClient != nil {
			resp, err := bc.grpcClient.Scan(context.Background(), &pb.ScanRequest{Prefix: bc.config.ScanPrefix, Limit: int32(bc.config.ScanLimit)})
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.Success {
				stats.Add(latency)
				totalOps++
			} else {
				errors++
			}
		} else {
			resp, err := bc.httpClient.Get(
				fmt.Sprintf("http://%s/api/v1/scan?prefix=%s&limit=%d", 
					bc.config.ServerAddr, bc.config.ScanPrefix, bc.config.ScanLimit),
			)
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.StatusCode != http.StatusOK {
				errors++
				resp.Body.Close()
				continue
			}
			var apiResp Response
			if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
				errors++
				resp.Body.Close()
				continue
			}
			resp.Body.Close()
			if !apiResp.Success {
				errors++
				continue
			}
			stats.Add(latency)
			totalOps++
		}
	}
	
	// Calculate statistics
	min, max, avg, p50, p95, p99 := stats.Calculate()
	
	result := &BenchmarkResult{
		Operation:     "SCAN",
		TotalOps:      totalOps,
		TotalTime:     time.Since(startTime),
		Throughput:    float64(totalOps) / time.Since(startTime).Seconds(),
		AvgLatency:    avg,
		MinLatency:    min,
		MaxLatency:    max,
		P50Latency:    p50,
		P95Latency:    p95,
		P99Latency:    p99,
		Errors:        errors,
		ErrorRate:     float64(errors) / float64(totalOps+errors) * 100,
	}
	
	resultChan <- result
}

// generateValue generates a test value of specified size
func generateValue(size int) string {
	if size <= 0 {
		return "test"
	}
	
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, size)
	for i := range result {
		result[i] = charset[i%len(charset)]
	}
	return string(result)
}

// printResult prints benchmark result
func printResult(result *BenchmarkResult) {
	fmt.Printf("\n=== %s Benchmark Results ===\n", result.Operation)
	fmt.Printf("Total Operations: %d\n", result.TotalOps)
	fmt.Printf("Total Time: %v\n", result.TotalTime)
	fmt.Printf("Throughput: %.2f ops/sec\n", result.Throughput)
	fmt.Printf("Average Latency: %v\n", result.AvgLatency)
	fmt.Printf("Min Latency: %v\n", result.MinLatency)
	fmt.Printf("Max Latency: %v\n", result.MaxLatency)
	fmt.Printf("P50 Latency: %v\n", result.P50Latency)
	fmt.Printf("P95 Latency: %v\n", result.P95Latency)
	fmt.Printf("P99 Latency: %v\n", result.P99Latency)
	fmt.Printf("Errors: %d (%.2f%%)\n", result.Errors, result.ErrorRate)
}

// printSummary prints summary of all results
func printSummary(results []*BenchmarkResult) {
	fmt.Printf("\n=== Benchmark Summary ===\n")
	fmt.Printf("%-10s %12s %12s %12s %12s %12s\n", 
		"Operation", "Throughput", "Avg Latency", "P95 Latency", "P99 Latency", "Error Rate")
	fmt.Printf("%-10s %12s %12s %12s %12s %12s\n", 
		"---------", "----------", "------------", "------------", "------------", "----------")
	
	for _, result := range results {
		fmt.Printf("%-10s %12.2f %12v %12v %12v %12.2f%%\n",
			result.Operation,
			result.Throughput,
			result.AvgLatency,
			result.P95Latency,
			result.P99Latency,
			result.ErrorRate)
	}
}

func main() {
	var (
		serverAddr    = flag.String("server", "127.0.0.1:8081", "Server address")
		grpcAddr      = flag.String("grpc-server", "", "gRPC server address (host:port)")
		useGrpc       = flag.Bool("grpc", false, "Use gRPC instead of HTTP")
		concurrency   = flag.Int("concurrency", 10, "Number of concurrent workers")
		duration      = flag.Duration("duration", 30*time.Second, "Benchmark duration")
		keyPrefix     = flag.String("key-prefix", "bench", "Key prefix for test data")
		valueSize     = flag.Int("value-size", 100, "Size of test values in bytes")
		scanPrefix    = flag.String("scan-prefix", "bench", "Prefix for scan operations")
		scanLimit     = flag.Int("scan-limit", 100, "Limit for scan operations")
		operations    = flag.String("operations", "set,get,scan", "Operations to benchmark (comma-separated)")
		reportInterval = flag.Duration("report-interval", 5*time.Second, "Progress report interval")
	)
	flag.Parse()
	config := &BenchmarkConfig{
		ServerAddr:     *serverAddr,
		GrpcAddr:       *grpcAddr,
		UseGrpc:        *useGrpc,
		Concurrency:    *concurrency,
		Duration:       *duration,
		KeyPrefix:      *keyPrefix,
		ValueSize:      *valueSize,
		ScanPrefix:     *scanPrefix,
		ScanLimit:      *scanLimit,
		ReportInterval: *reportInterval,
	}
	
	client := NewBenchmarkClient(config)
	
	fmt.Printf("=== Distributed Cache Benchmark ===\n")
	fmt.Printf("Server: %s\n", config.ServerAddr)
	fmt.Printf("Concurrency: %d\n", config.Concurrency)
	fmt.Printf("Duration: %v\n", config.Duration)
	fmt.Printf("Value Size: %d bytes\n", config.ValueSize)
	fmt.Printf("Operations: %s\n", *operations)
	fmt.Printf("Scan Prefix: %s, Limit: %d\n", config.ScanPrefix, config.ScanLimit)
	fmt.Printf("Report Interval: %v\n", config.ReportInterval)
	
	// Parse operations
	ops := make(map[string]bool)
	for _, op := range []string{"set", "get", "scan"} {
		ops[op] = false
	}
	
	for _, reqOp := range strings.Split(*operations, ",") {
		reqOp = strings.TrimSpace(reqOp)
		if reqOp == "set" || reqOp == "get" || reqOp == "scan" {
			ops[reqOp] = true
		}
	}
	
	var results []*BenchmarkResult
	// Calculate total number of goroutines
	totalGoroutines := 0
	if ops["set"] {
		totalGoroutines += config.Concurrency
	}
	if ops["get"] {
		totalGoroutines += config.Concurrency
	}
	if ops["scan"] {
		totalGoroutines += config.Concurrency
	}
	
	resultChan := make(chan *BenchmarkResult, totalGoroutines)
	var wg sync.WaitGroup
	
	// Start benchmarks
	if ops["set"] {
		for i := 0; i < config.Concurrency; i++ {
			wg.Add(1)
			go client.benchmarkSet(&wg, resultChan)
		}
	}
	
	if ops["get"] {
		for i := 0; i < config.Concurrency; i++ {
			wg.Add(1)
			go client.benchmarkGet(&wg, resultChan)
		}
	}
	
	if ops["scan"] {
		for i := 0; i < config.Concurrency; i++ {
			wg.Add(1)
			go client.benchmarkScan(&wg, resultChan)
		}
	}
	
	// Wait for completion
	wg.Wait()
	close(resultChan)
	
	// Collect results
	for result := range resultChan {
		results = append(results, result)
		printResult(result)
	}
	
	// Print summary
	printSummary(results)
} 