package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "dcache/proto"
)

func main() {
	fmt.Println("=== Simple gRPC Test ===")

	// 连接到服务器
	conn, err := grpc.Dial("127.0.0.1:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 创建客户端
	client := pb.NewDCacheClient(conn)

	// 设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 测试 Health 调用
	fmt.Println("Testing Health call...")
	resp, err := client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		fmt.Printf("Health call failed: %v\n", err)
		return
	}

	fmt.Printf("Health response: %+v\n", resp)
} 