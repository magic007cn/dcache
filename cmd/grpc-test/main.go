package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "dcache/proto"
)

func main() {
	// 解析命令行参数
	var addr string
	flag.StringVar(&addr, "addr", "127.0.0.1:8081", "gRPC server address")
	flag.Parse()

	// 连接到 gRPC 服务器
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", addr, err)
	}
	defer conn.Close()

	// 创建 gRPC 客户端
	client := pb.NewDCacheClient(conn)

	// 设置超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("=== DCache gRPC Test (connecting to %s) ===\n", addr)

	fmt.Println("=== DCache gRPC Test ===")

	// 测试 Set 操作
	fmt.Println("\n1. Testing SET operations...")
	testSet(client, ctx, "test_key1", "value1")
	testSet(client, ctx, "test_key2", "value2")
	testSet(client, ctx, "user:1", "john")
	testSet(client, ctx, "user:2", "jane")
	testSet(client, ctx, "config:debug", "true")

	// 测试 Get 操作
	fmt.Println("\n2. Testing GET operations...")
	testGet(client, ctx, "test_key1")
	testGet(client, ctx, "test_key2")
	testGet(client, ctx, "user:1")
	testGet(client, ctx, "nonexistent_key")

	// 测试 Scan 操作
	fmt.Println("\n3. Testing SCAN operations...")
	testScan(client, ctx, "test_", 10)
	testScan(client, ctx, "user:", 10)
	testScan(client, ctx, "config:", 10)

	// 测试 Delete 操作
	fmt.Println("\n4. Testing DELETE operations...")
	testDelete(client, ctx, "test_key1")
	testDelete(client, ctx, "user:1")

	// 验证删除后的状态
	fmt.Println("\n5. Verifying after DELETE...")
	testGet(client, ctx, "test_key1")
	testScan(client, ctx, "test_", 10)
	testScan(client, ctx, "user:", 10)

	// 测试 Status 和 Health
	fmt.Println("\n6. Testing STATUS and HEALTH...")
	testStatus(client, ctx)
	testHealth(client, ctx)

	fmt.Println("\n=== Test completed ===")
}

func testSet(client pb.DCacheClient, ctx context.Context, key, value string) {
	resp, err := client.Set(ctx, &pb.SetRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		fmt.Printf("SET %s failed: %v\n", key, err)
		return
	}
	if resp.Success {
		fmt.Printf("SET %s = %s ✓\n", key, value)
	} else {
		fmt.Printf("SET %s failed: %s\n", key, resp.Error)
	}
}

func testGet(client pb.DCacheClient, ctx context.Context, key string) {
	resp, err := client.Get(ctx, &pb.GetRequest{
		Key: key,
	})
	if err != nil {
		fmt.Printf("GET %s failed: %v\n", key, err)
		return
	}
	if resp.Success {
		fmt.Printf("GET %s = %s ✓\n", key, resp.Value)
	} else {
		fmt.Printf("GET %s failed: %s\n", key, resp.Error)
	}
}

func testDelete(client pb.DCacheClient, ctx context.Context, key string) {
	resp, err := client.Delete(ctx, &pb.DeleteRequest{
		Key: key,
	})
	if err != nil {
		fmt.Printf("DELETE %s failed: %v\n", key, err)
		return
	}
	if resp.Success {
		fmt.Printf("DELETE %s ✓\n", key)
	} else {
		fmt.Printf("DELETE %s failed: %s\n", key, resp.Error)
	}
}

func testScan(client pb.DCacheClient, ctx context.Context, prefix string, limit int32) {
	resp, err := client.Scan(ctx, &pb.ScanRequest{
		Prefix: prefix,
		Limit:  limit,
	})
	if err != nil {
		fmt.Printf("SCAN %s failed: %v\n", prefix, err)
		return
	}
	if resp.Success {
		fmt.Printf("SCAN %s (limit: %d):\n", prefix, limit)
		if len(resp.Pairs) == 0 {
			fmt.Printf("  No keys found\n")
		} else {
			for _, pair := range resp.Pairs {
				fmt.Printf("  %s = %s\n", pair.Key, pair.Value)
			}
		}
		fmt.Printf("  Total: %d keys ✓\n", len(resp.Pairs))
	} else {
		fmt.Printf("SCAN %s failed: %s\n", prefix, resp.Error)
	}
}

func testStatus(client pb.DCacheClient, ctx context.Context) {
	resp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		fmt.Printf("STATUS failed: %v\n", err)
		return
	}
	if resp.Success {
		fmt.Printf("STATUS:\n")
		fmt.Printf("  Leader: %s\n", resp.Leader)
		fmt.Printf("  Nodes: %v\n", resp.Nodes)
		fmt.Printf("  ✓\n")
	} else {
		fmt.Printf("STATUS failed: %s\n", resp.Error)
	}
}

func testHealth(client pb.DCacheClient, ctx context.Context) {
	resp, err := client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		fmt.Printf("HEALTH failed: %v\n", err)
		return
	}
	if resp.Success {
		fmt.Printf("HEALTH: %s ✓\n", resp.Status)
	} else {
		fmt.Printf("HEALTH failed: %s\n", resp.Error)
	}
} 