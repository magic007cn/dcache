# Distributed Cache Benchmark Tool

This document describes how to use the benchmark tool to test the performance of the distributed cache.

## Building the Benchmark Tool

```bash
make build
```

This will build the benchmark tool as `bin/benchmark`.

## Usage

### Basic Usage

```bash
# Basic benchmark with default settings
./bin/benchmark

# Benchmark with custom settings
./bin/benchmark -server 127.0.0.1:8081 -concurrency 20 -duration 60s
```

### Command Line Options

- `-server`: Server address (default: 127.0.0.1:8081)
- `-concurrency`: Number of concurrent workers (default: 10)
- `-duration`: Benchmark duration (default: 30s)
- `-key-prefix`: Key prefix for test data (default: bench)
- `-value-size`: Size of test values in bytes (default: 100)
- `-scan-prefix`: Prefix for scan operations (default: bench)
- `-scan-limit`: Limit for scan operations (default: 100)
- `-operations`: Operations to benchmark, comma-separated (default: set,get,scan)
- `-report-interval`: Progress report interval (default: 5s)

### Example Commands

```bash
# Test only SET operations
./bin/benchmark -operations set -concurrency 50 -duration 60s

# Test with larger values
./bin/benchmark -value-size 1024 -concurrency 20

# Test against different node
./bin/benchmark -server 127.0.0.1:8082

# Test specific operations
./bin/benchmark -operations set,get -concurrency 30

# High concurrency test
./bin/benchmark -concurrency 100 -duration 120s
```

## Benchmark Operations

### SET Operations
- Tests write performance
- Measures throughput and latency for key-value writes
- Uses configurable value sizes

### GET Operations
- Tests read performance
- Pre-populates 1000 keys for testing
- Measures throughput and latency for key-value reads

### SCAN Operations
- Tests range scan performance
- Uses configurable prefix and limit
- Measures throughput and latency for range queries

## Output Format

The benchmark tool provides detailed performance metrics:

```
=== SET Benchmark Results ===
Total Operations: 15000
Total Time: 30.5s
Throughput: 491.80 ops/sec
Average Latency: 2.03ms
Min Latency: 1.2ms
Max Latency: 15.7ms
P50 Latency: 1.8ms
P95 Latency: 3.2ms
P99 Latency: 8.1ms
Errors: 0 (0.00%)
```

### Metrics Explained

- **Throughput**: Operations per second
- **Average Latency**: Mean response time
- **P50/P95/P99**: Percentile latencies
- **Error Rate**: Percentage of failed operations

## Performance Testing Scenarios

### 1. Basic Performance Test
```bash
./bin/benchmark -duration 60s
```

### 2. High Concurrency Test
```bash
./bin/benchmark -concurrency 100 -duration 120s
```

### 3. Large Value Test
```bash
./bin/benchmark -value-size 4096 -concurrency 20
```

### 4. Read-Heavy Workload
```bash
./bin/benchmark -operations get -concurrency 50
```

### 5. Write-Heavy Workload
```bash
./bin/benchmark -operations set -concurrency 30
```

### 6. Mixed Workload
```bash
./bin/benchmark -operations set,get,scan -concurrency 40
```

## Tips for Accurate Benchmarking

1. **Warm up the cluster**: Run a few operations before starting the benchmark
2. **Monitor system resources**: Check CPU, memory, and network usage
3. **Test different nodes**: Benchmark against different cluster nodes
4. **Vary concurrency**: Test with different concurrency levels
5. **Test different value sizes**: Understand performance with different data sizes
6. **Run multiple times**: Get average results from multiple runs

## Interpreting Results

- **High throughput, low latency**: Good performance
- **High error rate**: System under stress or configuration issues
- **High P99 latency**: System may have bottlenecks
- **Low throughput**: System may be resource-constrained

## Troubleshooting

### Common Issues

1. **Connection refused**: Check if the server is running
2. **High error rate**: Reduce concurrency or check server logs
3. **Low throughput**: Check network latency and server resources
4. **Timeout errors**: Increase HTTP client timeout

### Debug Mode

For debugging, you can run with fewer concurrent workers:

```bash
./bin/benchmark -concurrency 1 -duration 10s
``` 