.PHONY: clean build run-node1 run-node2 run-node3 stop-cluster create-configs create-configs-node1 create-configs-node2 create-configs-node3 grpc-test

clean:
	@rm -rf bin/ data/ configs/
	@echo "Cleaned build artifacts and data directories."

build:
	go build -o bin/dcache cmd/dcache/main.go
	go build -o bin/dcache-client cmd/dcache-client/main.go
	go build -o bin/benchmark cmd/benchmark/main.go
	go build -o bin/grpc-test cmd/grpc-test/main.go

create-configs:
	@mkdir -p configs data/node1 data/node2 data/node3
	@cp config.yaml configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8081/g' configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9091/g' configs/node1.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092,node3=tcp:\/\/127.0.0.1:9093"/' configs/node1.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node1"|' configs/node1.yaml
	@echo '' >> configs/node1.yaml
	@echo 'node-id: "node1"' >> configs/node1.yaml

	@cp config.yaml configs/node2.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8082/g' configs/node2.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9092/g' configs/node2.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8082"/' configs/node2.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8082"/' configs/node2.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092,node3=tcp:\/\/127.0.0.1:9093"/' configs/node2.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node2"|' configs/node2.yaml
	@echo '' >> configs/node2.yaml
	@echo 'node-id: "node2"' >> configs/node2.yaml

	@cp config.yaml configs/node3.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8083/g' configs/node3.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9093/g' configs/node3.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8083"/' configs/node3.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8083"/' configs/node3.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9093"/' configs/node3.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9093"/' configs/node3.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092,node3=tcp:\/\/127.0.0.1:9093"/' configs/node3.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node3"|' configs/node3.yaml
	@echo '' >> configs/node3.yaml
	@echo 'node-id: "node3"' >> configs/node3.yaml

create-configs-node1:
	@mkdir -p configs data/node1
	@cp config.yaml configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8081/g' configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9091/g' configs/node1.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node1"|' configs/node1.yaml
	@sed -i '' 's/grpc-port: .*/grpc-port: 9097/' configs/node1.yaml
	@echo '' >> configs/node1.yaml
	@echo 'node-id: "node1"' >> configs/node1.yaml

create-configs-node2:
	@mkdir -p configs data/node1 data/node2
	@cp config.yaml configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8081/g' configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9091/g' configs/node1.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092"/' configs/node1.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node1"|' configs/node1.yaml
	@sed -i '' 's/grpc-port: .*/grpc-port: 9097/' configs/node1.yaml
	@echo '' >> configs/node1.yaml
	@echo 'node-id: "node1"' >> configs/node1.yaml

	@cp config.yaml configs/node2.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8082/g' configs/node2.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9092/g' configs/node2.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8082"/' configs/node2.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8082"/' configs/node2.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node2"|' configs/node2.yaml
	@sed -i '' 's/grpc-port: .*/grpc-port: 9098/' configs/node2.yaml
	@echo '' >> configs/node2.yaml
	@echo 'node-id: "node2"' >> configs/node2.yaml

create-configs-node3:
	@mkdir -p configs data/node1 data/node2 data/node3
	@cp config.yaml configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8081/g' configs/node1.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9091/g' configs/node1.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8081"/' configs/node1.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9091"/' configs/node1.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092,node3=tcp:\/\/127.0.0.1:9093"/' configs/node1.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node1"|' configs/node1.yaml
	@sed -i '' 's/grpc-port: .*/grpc-port: 9097/' configs/node1.yaml
	@echo '' >> configs/node1.yaml
	@echo 'node-id: "node1"' >> configs/node1.yaml

	@cp config.yaml configs/node2.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8082/g' configs/node2.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9092/g' configs/node2.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8082"/' configs/node2.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8082"/' configs/node2.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9092"/' configs/node2.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092,node3=tcp:\/\/127.0.0.1:9093"/' configs/node2.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node2"|' configs/node2.yaml
	@sed -i '' 's/grpc-port: .*/grpc-port: 9098/' configs/node2.yaml
	@echo '' >> configs/node2.yaml
	@echo 'node-id: "node2"' >> configs/node2.yaml

	@cp config.yaml configs/node3.yaml
	@sed -i '' 's/127.0.0.1:8080/127.0.0.1:8083/g' configs/node3.yaml
	@sed -i '' 's/127.0.0.1:8081/127.0.0.1:9093/g' configs/node3.yaml
	@sed -i '' 's/listen-client-urls: ".*"/listen-client-urls: "http:\/\/127.0.0.1:8083"/' configs/node3.yaml
	@sed -i '' 's/advertise-client-urls: ".*"/advertise-client-urls: "http:\/\/127.0.0.1:8083"/' configs/node3.yaml
	@sed -i '' 's/listen-peer-urls: ".*"/listen-peer-urls: "tcp:\/\/127.0.0.1:9093"/' configs/node3.yaml
	@sed -i '' 's/initial-advertise-peer-urls: ".*"/initial-advertise-peer-urls: "tcp:\/\/127.0.0.1:9093"/' configs/node3.yaml
	@sed -i '' 's/initial-cluster: .*/initial-cluster: "node1=tcp:\/\/127.0.0.1:9091,node2=tcp:\/\/127.0.0.1:9092,node3=tcp:\/\/127.0.0.1:9093"/' configs/node3.yaml
	@sed -i '' 's|data-dir: ".*"|data-dir: "./data/node3"|' configs/node3.yaml
	@sed -i '' 's/grpc-port: .*/grpc-port: 9099/' configs/node3.yaml
	@echo '' >> configs/node3.yaml
	@echo 'node-id: "node3"' >> configs/node3.yaml

run-node1: build create-configs-node1
	@echo "Starting node1..."
	@./bin/dcache --config configs/node1.yaml --data-dir data/node1 &

run-node2: build create-configs-node2
	@echo "Starting node1 and node2..."
	@./bin/dcache --config configs/node1.yaml --data-dir data/node1 &
	@sleep 2
	@./bin/dcache --config configs/node2.yaml --data-dir data/node2 &

run-node3: build create-configs-node3
	@echo "Starting node1, node2, and node3..."
	@./bin/dcache --config configs/node1.yaml --data-dir data/node1 &
	@sleep 2
	@./bin/dcache --config configs/node2.yaml --data-dir data/node2 &
	@sleep 2
	@./bin/dcache --config configs/node3.yaml --data-dir data/node3 &

stop-cluster:
	@pkill -f dcache || true
	@echo "All dcache nodes stopped."

grpc-test: build
	@echo "Running gRPC test..."
	@./bin/grpc-test 