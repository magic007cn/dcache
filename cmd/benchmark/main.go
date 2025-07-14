package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"io"

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
	// 配置 HTTP 连接池和复用 - 长连接优化配置
	transport := &http.Transport{
		MaxIdleConns:        100,             // 大幅增加最大空闲连接数
		MaxIdleConnsPerHost: 100,             // 大幅增加每个主机的最大空闲连接数
		IdleConnTimeout:     5 * time.Second, // 大幅增加空闲连接超时，保持长连接
		DisableCompression:  true,             // 禁用压缩以提高性能
		DisableKeepAlives:   false,            // 启用 Keep-Alive
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second, // 增加连接超时
			KeepAlive: 5 * time.Second, // 大幅增加 Keep-Alive 间隔，保持长连接
		}).DialContext,
		ForceAttemptHTTP2: false, // 强制使用 HTTP/1.1
		MaxConnsPerHost:   100,  // 大幅增加每个主机的最大连接数
	}
	
	bc := &BenchmarkClient{
		config: config,
		httpClient: &http.Client{
			Timeout:   5 * time.Second, // 增加请求超时
			Transport: transport,
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

// 通用API响应结构
type APIResponse struct {
	Header       struct{} `json:"header"`
	SuccessCount int      `json:"success_count"`
	ErrorCount   int      `json:"error_count"`
	Code         int      `json:"code"`
	Error        string   `json:"error"`
	KVs          []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"kvs"`
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
				if errors <= 10 { // 只打印前10个错误，避免日志过多
					fmt.Printf("gRPC SET error for key %s: %v\n", key, err)
				}
				continue
			}
			stats.Add(latency)
			totalOps++
		} else {
			// HTTP
			// set: PUT /api/v1/keys/{key}，body只需value字段
			jsonData, _ := json.Marshal(map[string]string{"value": value})
			start := time.Now()
			req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/api/v1/keys/%s", bc.config.ServerAddr, key), bytes.NewBuffer(jsonData))
			req.Header.Set("Content-Type", "application/json")
			// 移除显式的 Connection: keep-alive 设置，使用 Go HTTP 客户端的默认行为
			resp, err := bc.httpClient.Do(req)
			latency := time.Since(start)
			if err != nil {
				errors++
				if errors <= 10 {
					fmt.Printf("HTTP SET network error for key %s: %v\n", key, err)
				}
				continue
			}
			
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				var apiResp APIResponse
				_ = json.NewDecoder(resp.Body).Decode(&apiResp)
				resp.Body.Close()
				errors++
				if errors <= 10 {
					fmt.Printf("HTTP SET error for key %s: status=%d, error=%s, code=%d\n", 
						key, resp.StatusCode, apiResp.Error, apiResp.Code)
				}
				continue
			}
			// 成功响应也需要读取并关闭，确保连接可复用
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			stats.Add(latency)
			totalOps++
		}
	}
	// Calculate statistics
	min, max, avg, p50, p95, p99 := stats.Calculate()
	fmt.Printf("LOOP END: totalOps=%d, errors=%d\n", totalOps, errors)
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
	fmt.Printf("DEBUG: SET final stats - totalOps: %d, errors: %d, errorRate: %.2f%%\n", 
		totalOps, errors, float64(errors) / float64(totalOps+errors) * 100)
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
			// HTTP 预置数据也用PUT
			jsonData, _ := json.Marshal(map[string]string{"value": value})
			req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/api/v1/keys/%s", bc.config.ServerAddr, key), bytes.NewBuffer(jsonData))
			req.Header.Set("Content-Type", "application/json")
			resp, err := bc.httpClient.Do(req)
			if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
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
				fmt.Sprintf("http://%s/api/v1/keys/%s", bc.config.ServerAddr, key),
			)
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				var apiResp APIResponse
				_ = json.NewDecoder(resp.Body).Decode(&apiResp)
				resp.Body.Close()
				errors++
				continue
			}
			var apiResp APIResponse
			if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
				resp.Body.Close()
				errors++
				continue
			}
			resp.Body.Close()
			if apiResp.Code != 0 || apiResp.SuccessCount == 0 || len(apiResp.KVs) == 0 {
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

// benchmarkRangeScan performs RANGE SCAN operation benchmark
func (bc *BenchmarkClient) benchmarkRangeScan(wg *sync.WaitGroup, resultChan chan<- *BenchmarkResult) {
	defer wg.Done()
	stats := &LatencyStats{}
	var totalOps int64
	var errors int64
	// First, set some data for RANGE SCAN testing (before timing)
	value := generateValue(bc.config.ValueSize)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%s_range_scan_%d", bc.config.KeyPrefix, i)
		if bc.config.UseGrpc && bc.grpcClient != nil {
			_, _ = bc.grpcClient.Set(context.Background(), &pb.SetRequest{Key: key, Value: value})
		} else {
			// HTTP 预置数据也用PUT
			jsonData, _ := json.Marshal(map[string]string{"value": value})
			req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/api/v1/keys/%s", bc.config.ServerAddr, key), bytes.NewBuffer(jsonData))
			req.Header.Set("Content-Type", "application/json")
			resp, err := bc.httpClient.Do(req)
			if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				resp.Body.Close()
			}
		}
	}
	// Now benchmark RANGE SCAN operations
	startTime := time.Now()
	for time.Since(startTime) < bc.config.Duration {
		start := time.Now()
		if bc.config.UseGrpc && bc.grpcClient != nil {
			resp, err := bc.grpcClient.RangeScan(context.Background(), &pb.RangeScanRequest{Prefix: bc.config.ScanPrefix, Limit: int32(bc.config.ScanLimit)})
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.Code == 0 {
				stats.Add(latency)
				totalOps++
			} else {
				errors++
			}
		} else {
			resp, err := bc.httpClient.Get(
				fmt.Sprintf("http://%s/api/v1/keys?prefix=%s&limit=%d", 
					bc.config.ServerAddr, bc.config.ScanPrefix, bc.config.ScanLimit),
			)
			latency := time.Since(start)
			if err != nil {
				errors++
				continue
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				var apiResp APIResponse
				_ = json.NewDecoder(resp.Body).Decode(&apiResp)
				resp.Body.Close()
				errors++
				continue
			}
			var apiResp APIResponse
			if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
				resp.Body.Close()
				errors++
				continue
			}
			resp.Body.Close()
			if apiResp.Code != 0 || apiResp.SuccessCount == 0 || len(apiResp.KVs) == 0 {
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
		Operation:     "RANGE_SCAN",
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
		operations    = flag.String("operations", "set,get,range-scan", "Operations to benchmark (comma-separated)")
		reportInterval = flag.Duration("report-interval", 5*time.Second, "Progress report interval")
		batchSize     = flag.Int("batch-size", 0, "Batch size for batch operations (0 = single operations)")
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
	
	// 如果没有指定gRPC地址，使用HTTP服务器地址
	if config.UseGrpc && config.GrpcAddr == "" {
		config.GrpcAddr = config.ServerAddr
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
	if *batchSize > 0 {
		fmt.Printf("Batch Size: %d\n", *batchSize)
	}
	
	// If batch mode is enabled, run batch benchmark
	if *batchSize > 0 {
		runBatchBenchmark(client, *batchSize, *duration)
		return
	}
	
	// Parse operations
	ops := make(map[string]bool)
	for _, op := range []string{"set", "get", "range-scan"} {
		ops[op] = false
	}
	
	for _, reqOp := range strings.Split(*operations, ",") {
		reqOp = strings.TrimSpace(reqOp)
		if reqOp == "set" || reqOp == "get" || reqOp == "range-scan" {
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
	if ops["range-scan"] {
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
	
	if ops["range-scan"] {
		for i := 0; i < config.Concurrency; i++ {
			wg.Add(1)
			go client.benchmarkRangeScan(&wg, resultChan)
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

func runBatchBenchmark(client *BenchmarkClient, batchSize int, duration time.Duration) {
	var totalOps, totalErrors int64
	var latencies []time.Duration
	start := time.Now()
	
	fmt.Printf("Starting batch benchmark with batch size %d...\n", batchSize)
	
	for time.Since(start) < duration {
		var pairs []*pb.KeyValue
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("batch-%d-%d", start.UnixNano(), i)
			value := generateValue(client.config.ValueSize)
			pairs = append(pairs, &pb.KeyValue{Key: key, Value: value})
		}
		
		batchStart := time.Now()
		if client.config.UseGrpc {
			resp, err := client.grpcClient.BatchSet(context.Background(), &pb.BatchSetRequest{Pairs: pairs})
			latency := time.Since(batchStart)
			latencies = append(latencies, latency)
			
			if err != nil || resp.Code != 0 {
				totalErrors += int64(batchSize)
				if totalErrors <= 10 { // 只打印前10个错误
					fmt.Printf("BatchSet error: %v\n", err)
				}
			} else {
				totalOps += int64(resp.SuccessCount)
				totalErrors += int64(resp.ErrorCount)
			}
		} else {
			// Convert protobuf KeyValue to API KeyValue format
			var apiPairs []map[string]string
			for _, kv := range pairs {
				apiPairs = append(apiPairs, map[string]string{
					"key":   kv.Key,
					"value": kv.Value,
				})
			}
			req := map[string]interface{}{ "pairs": apiPairs }
			jsonData, _ := json.Marshal(req)
			resp, err := client.httpClient.Post(
				fmt.Sprintf("http://%s/api/v1/keys", client.config.ServerAddr),
				"application/json", bytes.NewBuffer(jsonData))
			
			latency := time.Since(batchStart)
			latencies = append(latencies, latency)
			
			if err != nil || resp.StatusCode != http.StatusOK {
				totalErrors += int64(batchSize)
				if totalErrors <= 10 {
					fmt.Printf("BatchSet HTTP error: %v\n", err)
				}
				continue
			}
			
			var apiResp APIResponse
			if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
				totalErrors += int64(batchSize)
				if totalErrors <= 10 {
					fmt.Printf("BatchSet HTTP response parse error: %v\n", err)
				}
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			if apiResp.Code != 0 {
				totalErrors += int64(batchSize)
				if totalErrors <= 10 {
					fmt.Printf("BatchSet HTTP API error: code=%d, error=%s\n", apiResp.Code, apiResp.Error)
				}
			} else {
				totalOps += int64(apiResp.SuccessCount)
				totalErrors += int64(apiResp.ErrorCount)
			}
		}
	}
	
	totalTime := time.Since(start)
	throughput := float64(totalOps) / totalTime.Seconds()
	
	// Calculate latency statistics
	if len(latencies) > 0 {
		// Sort latencies for percentile calculation
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})
		
		minLatency := latencies[0]
		maxLatency := latencies[len(latencies)-1]
		
		// Calculate average latency
		var totalLatency time.Duration
		for _, l := range latencies {
			totalLatency += l
		}
		avgLatency := totalLatency / time.Duration(len(latencies))
		
		// Calculate percentiles
		p50Idx := len(latencies) * 50 / 100
		p95Idx := len(latencies) * 95 / 100
		p99Idx := len(latencies) * 99 / 100
		
		p50Latency := latencies[p50Idx]
		p95Latency := latencies[p95Idx]
		p99Latency := latencies[p99Idx]
		
		errorRate := float64(totalErrors) / float64(totalOps+totalErrors) * 100
		
		fmt.Printf("\n=== BATCH_SET Benchmark Results ===\n")
		fmt.Printf("Total Operations: %d\n", totalOps)
		fmt.Printf("Total Time: %v\n", totalTime)
		fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
		fmt.Printf("Average Latency: %v\n", avgLatency)
		fmt.Printf("Min Latency: %v\n", minLatency)
		fmt.Printf("Max Latency: %v\n", maxLatency)
		fmt.Printf("P50 Latency: %v\n", p50Latency)
		fmt.Printf("P95 Latency: %v\n", p95Latency)
		fmt.Printf("P99 Latency: %v\n", p99Latency)
		fmt.Printf("Errors: %d (%.2f%%)\n", totalErrors, errorRate)
		
		fmt.Printf("\n=== Benchmark Summary ===\n")
		fmt.Printf("Operation    Throughput  Avg Latency  P95 Latency  P99 Latency   Error Rate\n")
		fmt.Printf("---------    ---------- ------------ ------------ ------------   ----------\n")
		fmt.Printf("%-10s %12.2f %12v %12v %12v %12.2f%%\n",
			"BATCH_SET",
			throughput,
			avgLatency,
			p95Latency,
			p99Latency,
			errorRate)
	} else {
		fmt.Printf("No successful operations recorded.\n")
	}
}

func runStreamBenchmark(client *BenchmarkClient, batchSize int, duration time.Duration) {
	var totalOps, totalErrors int64
	start := time.Now()
	for time.Since(start) < duration {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := client.grpcClient.StreamSet(ctx)
		if err != nil {
			fmt.Printf("StreamSet error: %v\n", err)
			cancel()
			continue
		}
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("stream-%d-%d", start.UnixNano(), i)
			value := generateValue(client.config.ValueSize)
			if err := stream.Send(&pb.SetRequest{Key: key, Value: value}); err != nil {
				totalErrors++
			}
		}
		resp, err := stream.CloseAndRecv()
		if err != nil {
			fmt.Printf("StreamSet close error: %v\n", err)
			totalErrors += int64(batchSize)
		} else {
			totalOps += int64(resp.SuccessCount)
			totalErrors += int64(resp.ErrorCount)
		}
		cancel()
	}
	fmt.Printf("StreamSet: TotalOps=%d, TotalErrors=%d\n", totalOps, totalErrors)
}

func runSingleBenchmark(client *BenchmarkClient, duration time.Duration) {
	// 原有的单条set/get/scan逻辑
	client.benchmarkSet(nil, nil)
	client.benchmarkGet(nil, nil)
	client.benchmarkRangeScan(nil, nil)
} 