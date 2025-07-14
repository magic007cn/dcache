package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "dcache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrpcClient struct {
	conn   *grpc.ClientConn
	client pb.DCacheClient
}

func NewGrpcClient(addr string) (*GrpcClient, error) {
	// Create a single connection that will be reused
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	client := pb.NewDCacheClient(conn)
	return &GrpcClient{
		conn:   conn,
		client: client,
	}, nil
}

func (c *GrpcClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *GrpcClient) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("set failed: %v", err)
	}

	if resp.Code != 0 {
		return fmt.Errorf("set failed: %s", resp.Error)
	}

	return nil
}

func (c *GrpcClient) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", fmt.Errorf("get failed: %v", err)
	}

	if resp.Code != 0 {
		return "", fmt.Errorf("get failed: %s", resp.Error)
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("key not found")
	}
	return resp.Kvs[0].Value, nil
}

func (c *GrpcClient) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("delete failed: %v", err)
	}

	if resp.Code != 0 {
		return fmt.Errorf("delete failed: %s", resp.Error)
	}

	return nil
}

func (c *GrpcClient) BatchSet(pairs []*pb.KeyValue) (*pb.BatchSetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.BatchSet(ctx, &pb.BatchSetRequest{Pairs: pairs})
	if err != nil {
		return nil, fmt.Errorf("batch set failed: %v", err)
	}

	if resp.Code != 0 {
		return nil, fmt.Errorf("batch set failed: %s", resp.Error)
	}

	return resp, nil
}

func (c *GrpcClient) BatchGet(keys []string) (*pb.BatchGetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.BatchGet(ctx, &pb.BatchGetRequest{Keys: keys})
	if err != nil {
		return nil, fmt.Errorf("batch get failed: %v", err)
	}

	// 即使有部分错误也返回结果，让调用者处理
	return resp, nil
}

func (c *GrpcClient) BatchDelete(keys []string) (*pb.BatchDeleteResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.client.BatchDelete(ctx, &pb.BatchDeleteRequest{Keys: keys})
	if err != nil {
		return nil, fmt.Errorf("batch delete failed: %v", err)
	}

	if resp.Code != 0 {
		return nil, fmt.Errorf("batch delete failed: %s", resp.Error)
	}

	return resp, nil
}

func (c *GrpcClient) Status() (*pb.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("status failed: %v", err)
	}

	if resp.Code != 0 {
		return nil, fmt.Errorf("status failed: %s", resp.Error)
	}

	return resp, nil
}

func (c *GrpcClient) Health() (*pb.HealthResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return nil, fmt.Errorf("health check failed: %v", err)
	}

	if resp.Code != 0 {
		return nil, fmt.Errorf("health check failed: %s", resp.Error)
	}

	return resp, nil
}

func main() {
	// Create gRPC client with connection reuse
	client, err := NewGrpcClient("127.0.0.1:8081")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test health check
	fmt.Println("=== Health Check ===")
	health, err := client.Health()
	if err != nil {
		log.Printf("Health check failed: %v", err)
	} else {
		fmt.Printf("Health: %s\n", health.Status)
	}

	// Test status
	fmt.Println("\n=== Status ===")
	status, err := client.Status()
	if err != nil {
		log.Printf("Status failed: %v", err)
	} else {
		fmt.Printf("Leader: %s\n", status.Leader)
		fmt.Printf("Nodes: %v\n", status.Nodes)
	}

	// Test single operations
	fmt.Println("\n=== Single Operations ===")
	
	// Set
	err = client.Set("test-key", "test-value")
	if err != nil {
		log.Printf("Set failed: %v", err)
	} else {
		fmt.Println("Set: OK")
	}

	// Get
	value, err := client.Get("test-key")
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else {
		fmt.Printf("Get: %s\n", value)
	}

	// Test batch operations
	fmt.Println("\n=== Batch Operations ===")
	
	// Batch Set
	batchPairs := []*pb.KeyValue{
		{Key: "batch-key1", Value: "batch-value1"},
		{Key: "batch-key2", Value: "batch-value2"},
		{Key: "batch-key3", Value: "batch-value3"},
	}
	
	batchSetResp, err := client.BatchSet(batchPairs)
	if err != nil {
		log.Printf("Batch set failed: %v", err)
	} else {
		fmt.Printf("Batch Set: SuccessCount=%d, ErrorCount=%d\n", 
			batchSetResp.SuccessCount, batchSetResp.ErrorCount)
		if len(batchSetResp.Errors) > 0 {
			fmt.Printf("Errors: %v\n", batchSetResp.Errors)
		}
	}

	// Batch Get
	batchKeys := []string{"batch-key1", "batch-key2", "batch-key3", "nonexistent-key"}
	batchGetResp, err := client.BatchGet(batchKeys)
	if err != nil {
		log.Printf("Batch get failed: %v", err)
	} else {
		fmt.Printf("Batch Get: SuccessCount=%d, ErrorCount=%d\n", 
			batchGetResp.SuccessCount, batchGetResp.ErrorCount)
		fmt.Printf("Pairs: %v\n", batchGetResp.Kvs)
		if len(batchGetResp.Errors) > 0 {
			fmt.Printf("Errors: %v\n", batchGetResp.Errors)
		}
	}

	// Batch Delete
	batchDeleteKeys := []string{"batch-key1", "batch-key2", "nonexistent-key"}
	batchDeleteResp, err := client.BatchDelete(batchDeleteKeys)
	if err != nil {
		log.Printf("Batch delete failed: %v", err)
	} else {
		fmt.Printf("Batch Delete: SuccessCount=%d, ErrorCount=%d\n", 
			batchDeleteResp.SuccessCount, batchDeleteResp.ErrorCount)
		if len(batchDeleteResp.Errors) > 0 {
			fmt.Printf("Errors: %v\n", batchDeleteResp.Errors)
		}
	}

	fmt.Println("\n=== Test Completed ===")
} 