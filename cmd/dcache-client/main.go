package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
)

type NodeRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func main() {
	var (
		serverAddr = flag.String("server", "127.0.0.1:8081", "Server address")
		action     = flag.String("action", "", "Action: add-node, remove-node, status")
		nodeID     = flag.String("node-id", "", "Node ID")
		address    = flag.String("address", "", "Node address (for add-node)")
	)
	flag.Parse()

	if *action == "" {
		fmt.Println("Usage: dcache-client -action <action> [options]")
		fmt.Println("Actions:")
		fmt.Println("  add-node    - Add a new node to cluster")
		fmt.Println("  remove-node - Remove a node from cluster")
		fmt.Println("  status      - Show cluster status")
		fmt.Println("")
		fmt.Println("Examples:")
		fmt.Println("  dcache-client -action add-node -node-id node4 -address tcp://127.0.0.1:9094")
		fmt.Println("  dcache-client -action remove-node -node-id node3")
		fmt.Println("  dcache-client -action status")
		os.Exit(1)
	}

	switch *action {
	case "add-node":
		if *nodeID == "" || *address == "" {
			fmt.Println("Error: node-id and address are required for add-node action")
			os.Exit(1)
		}
		addNode(*serverAddr, *nodeID, *address)
	case "remove-node":
		if *nodeID == "" {
			fmt.Println("Error: node-id is required for remove-node action")
			os.Exit(1)
		}
		removeNode(*serverAddr, *nodeID)
	case "status":
		showStatus(*serverAddr)
	default:
		fmt.Printf("Unknown action: %s\n", *action)
		os.Exit(1)
	}
}

func addNode(serverAddr, nodeID, address string) {
	req := NodeRequest{
		NodeID:  nodeID,
		Address: address,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("Error marshaling request: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/cluster/add-node", serverAddr),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		fmt.Printf("Error adding node: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		os.Exit(1)
	}

	if response.Success {
		fmt.Printf("Success: %v\n", response.Data)
	} else {
		fmt.Printf("Error: %s\n", response.Error)
		os.Exit(1)
	}
}

func removeNode(serverAddr, nodeID string) {
	req := NodeRequest{
		NodeID: nodeID,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("Error marshaling request: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/cluster/remove-node", serverAddr),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		fmt.Printf("Error removing node: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		os.Exit(1)
	}

	if response.Success {
		fmt.Printf("Success: %v\n", response.Data)
	} else {
		fmt.Printf("Error: %s\n", response.Error)
		os.Exit(1)
	}
}

func showStatus(serverAddr string) {
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/cluster/status", serverAddr))
	if err != nil {
		fmt.Printf("Error getting status: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		os.Exit(1)
	}

	if response.Success {
		status := response.Data.(map[string]interface{})
		fmt.Printf("Cluster Status:\n")
		fmt.Printf("  Node ID: %v\n", status["node_id"])
		fmt.Printf("  State: %v\n", status["state"])
		fmt.Printf("  Is Leader: %v\n", status["is_leader"])
		fmt.Printf("  Leader: %v\n", status["leader"])
	} else {
		fmt.Printf("Error: %s\n", response.Error)
		os.Exit(1)
	}
} 