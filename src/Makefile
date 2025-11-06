.PHONY: help proto services test docker k8s clean

# Configuration
PROTO_DIR := api/proto
GO_OUT_DIR := api/gen/go
PYTHON_OUT_DIR := api/gen/python
SERVICES := scheduler plc-adapter-p1 plc-adapter-p2 engraver-adapter hmi-service flash-station release-policy device-registry validation-controller quality-service template-service genealogy-service

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

#############
# Protobuf
#############

proto: proto-go proto-python ## Generate all protobuf code

proto-go: ## Generate Go protobuf code
	@echo "Generating Go protobuf files..."
	@mkdir -p $(GO_OUT_DIR)
	@protoc -I $(PROTO_DIR) \
		--go_out=$(GO_OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(shell find $(PROTO_DIR) -name "*.proto")

proto-python: ## Generate Python protobuf code
	@echo "Generating Python protobuf files..."
	@mkdir -p $(PYTHON_OUT_DIR)
	@$(VIRTUAL_ENV)/bin/python -m grpc_tools.protoc -I $(PROTO_DIR) \
		--python_out=$(PYTHON_OUT_DIR) \
		--grpc_python_out=$(PYTHON_OUT_DIR) \
		$(shell find $(PROTO_DIR) -name "*.proto")

#############
# Development
#############

install-tools: ## Install required tools
	@echo "Installing tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	pip install grpcio-tools

go-mod-tidy: ## Tidy Go modules
	@for service in $(SERVICES); do \
		echo "Tidying $$service..."; \
		cd services/go/$$service && go mod tidy && cd ../../..; \
	done

#############
# Local Infrastructure
#############

infra-up: ## Start local infrastructure (Docker Compose)
	docker-compose -f docker/docker-compose.infra.yml up -d

infra-down: ## Stop local infrastructure
	docker-compose -f docker/docker-compose.infra.yml down

infra-logs: ## Show infrastructure logs
	docker-compose -f docker/docker-compose.infra.yml logs -f

#############
# Services
#############

services-build: ## Build all Go services
	@for service in $(SERVICES); do \
		echo "Building $$service..."; \
		cd services/go/$$service && go build -o ../../../bin/$$service ./cmd/server && cd ../../..; \
	done

services-up: services-build ## Start all services locally
	@echo "Starting services..."
	@./scripts/start-services.sh

services-down: ## Stop all services
	@./scripts/stop-services.sh

#############
# Testing
#############

test-unit: ## Run unit tests
	@echo "Running unit tests..."
	@for service in $(SERVICES); do \
		echo "Testing $$service..."; \
		cd services/go/$$service && go test ./... -v -cover && cd ../../..; \
	done

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	go test ./tests/integration/... -v -tags=integration

test-e2e: ## Run end-to-end tests
	@echo "Running E2E tests..."
	go test ./tests/e2e/... -v -tags=e2e

test-load: ## Run load tests
	@echo "Running load tests..."
	k6 run tests/load/production-plan.js

#############
# Docker
#############

docker-build: ## Build all Docker images
	@for service in $(SERVICES); do \
		echo "Building Docker image for $$service..."; \
		docker build -t i40-$$service:latest -f docker/Dockerfile.go --build-arg SERVICE=$$service .; \
	done
	@echo "Building Python services..."
	docker build -t i40-weather-aggregator:latest -f docker/Dockerfile.python --build-arg SERVICE=weather-aggregator .
	docker build -t i40-data-quality:latest -f docker/Dockerfile.python --build-arg SERVICE=data-quality .

docker-push: ## Push Docker images to registry
	@echo "Pushing images to registry..."
	@./scripts/docker-push.sh

#############
# Kubernetes
#############

k8s-install-deps: ## Install K8s dependencies (Kafka, PostgreSQL, etc.)
	@echo "Installing dependencies..."
	kubectl create namespace i40-production || true
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	@echo "Installing Kafka..."
	helm install kafka bitnami/kafka -n i40-production -f k8s/helm-values/kafka-values.yaml
	@echo "Installing PostgreSQL..."
	helm install postgresql bitnami/postgresql -n i40-production -f k8s/helm-values/postgresql-values.yaml
	@echo "Installing Redis..."
	helm install redis bitnami/redis -n i40-production -f k8s/helm-values/redis-values.yaml
	@echo "Installing Prometheus..."
	helm install prometheus prometheus-community/kube-prometheus-stack -n i40-production

k8s-deploy: docker-build ## Deploy to Kubernetes
	@echo "Deploying to Kubernetes..."
	kubectl apply -k k8s/overlays/dev

k8s-deploy-blue: ## Deploy to blue environment
	@echo "Deploying to blue environment..."
	kubectl apply -k k8s/blue-green/blue

k8s-deploy-green: ## Deploy to green environment
	@echo "Deploying to green environment..."
	kubectl apply -k k8s/blue-green/green

k8s-cutover-blue: ## Switch traffic to blue
	@echo "Switching traffic to blue..."
	./scripts/blue-green-cutover.sh blue

k8s-cutover-green: ## Switch traffic to green (rollback)
	@echo "Switching traffic to green..."
	./scripts/blue-green-cutover.sh green

k8s-status: ## Check K8s deployment status
	kubectl get pods -n i40-production

k8s-logs: ## Tail logs from all services
	kubectl logs -n i40-production -l app.kubernetes.io/part-of=i40-production -f --max-log-requests=20

k8s-port-forward: ## Port forward to services
	@echo "Port forwarding..."
	kubectl port-forward -n i40-production svc/grafana 3000:3000 &
	kubectl port-forward -n i40-production svc/prometheus 9090:9090 &
	kubectl port-forward -n i40-production svc/jaeger 16686:16686 &

#############
# Cleanup
#############

clean: ## Clean generated files
	rm -rf $(GO_OUT_DIR) $(PYTHON_OUT_DIR) bin/

clean-k8s: ## Clean K8s resources
	kubectl delete namespace i40-production

#############
# Monitoring
#############

logs: ## View logs from all services (Docker Compose)
	docker-compose -f docker/docker-compose.infra.yml logs -f

prometheus-ui: ## Open Prometheus UI
	open http://localhost:9090

grafana-ui: ## Open Grafana UI
	open http://localhost:3000

jaeger-ui: ## Open Jaeger UI
	open http://localhost:16686
