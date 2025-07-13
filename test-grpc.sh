#!/bin/bash

echo "=== DCache gRPC Test Script ==="

# 清理之前的进程
echo "Cleaning up previous processes..."
pkill -f dcache || true
sleep 2

# 清理数据目录
echo "Cleaning data directories..."
rm -rf data/node1/* data/node2/* data/node3/* 2>/dev/null || true

# 启动单节点集群
echo "Starting single node cluster..."
make run-node1

# 等待节点启动
echo "Waiting for node to start..."
sleep 5

# 运行 gRPC 测试
echo "Running gRPC test..."
./bin/grpc-test

# 停止集群
echo "Stopping cluster..."
make stop-cluster

echo "=== Test completed ===" 