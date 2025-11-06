#!/bin/bash
set -e

# I4.0 Production System - Automated Setup Script
# This script sets up the complete development environment

echo "======================================"
echo "I4.0 Production System Setup"
echo "======================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo "Checking prerequisites..."

command -v docker >/dev/null 2>&1 || { echo -e "${RED}Docker is not installed${NC}"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo -e "${RED}kubectl is not installed${NC}"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo -e "${RED}Helm is not installed${NC}"; exit 1; }
command -v go >/dev/null 2>&1 || { echo -e "${RED}Go is not installed${NC}"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo -e "${RED}Python3 is not installed${NC}"; exit 1; }
command -v protoc >/dev/null 2>&1 || { echo -e "${RED}protoc is not installed${NC}"; exit 1; }

echo -e "${GREEN}✓ All prerequisites installed${NC}"
echo ""

# Setup mode selection
echo "Select setup mode:"
echo "1) Local development (Docker Compose)"
echo "2) Kubernetes deployment"
echo "3) Complete setup (both)"
read -p "Enter choice [1-3]: " choice

case $choice in
    1)
        MODE="local"
        ;;
    2)
        MODE="k8s"
        ;;
    3)
        MODE="both"
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${YELLOW}Setting up in ${MODE} mode...${NC}"
echo ""

# Step 1: Generate Protobuf code
echo "Step 1: Generating protobuf code..."
make proto || { echo -e "${RED}Failed to generate protobuf code${NC}"; exit 1; }
echo -e "${GREEN}✓ Protobuf code generated${NC}"
echo ""

# Step 2: Setup local development
if [[ "$MODE" == "local" || "$MODE" == "both" ]]; then
    echo "Step 2: Setting up local infrastructure..."

    # Create necessary directories
    mkdir -p docker/prometheus docker/grafana/provisioning docker/timescaledb

    # Create Prometheus config
    cat > docker/prometheus/prometheus.yml <<EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'i40-services'
    static_configs:
      - targets: ['scheduler:8080', 'device-registry:8081']
    metrics_path: '/metrics'
EOF

    # Create TimescaleDB init script
    cat > docker/timescaledb/init.sql <<EOF
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    sensor_name TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT
);

SELECT create_hypertable('telemetry', 'time', if_not_exists => TRUE);

CREATE INDEX idx_telemetry_device ON telemetry (device_id, time DESC);
EOF

    # Start infrastructure
    echo "Starting infrastructure services..."
    docker-compose -f docker/docker-compose.infra.yml up -d

    echo "Waiting for services to be ready..."
    sleep 30

    # Check service health
    echo "Checking service health..."
    docker-compose -f docker/docker-compose.infra.yml ps

    echo -e "${GREEN}✓ Local infrastructure ready${NC}"
    echo ""
    echo "Services available at:"
    echo "  - Kafka UI: http://localhost:8080"
    echo "  - Prometheus: http://localhost:9090"
    echo "  - Grafana: http://localhost:3000 (admin/admin)"
    echo "  - Jaeger: http://localhost:16686"
    echo "  - Vault: http://localhost:8200 (token: root)"
    echo ""
fi

# Step 3: Setup Kubernetes
if [[ "$MODE" == "k8s" || "$MODE" == "both" ]]; then
    echo "Step 3: Setting up Kubernetes environment..."

    # Check if Kubernetes is running
    kubectl cluster-info >/dev/null 2>&1 || { echo -e "${RED}Kubernetes cluster not accessible${NC}"; exit 1; }

    # Create namespace
    kubectl create namespace i40-production --dry-run=client -o yaml | kubectl apply -f -

    # Install dependencies
    echo "Installing Helm charts..."
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update

    # Install Kafka
    echo "Installing Kafka..."
    cat > /tmp/kafka-values.yaml <<EOF
replicaCount: 1
zookeeper:
  replicaCount: 1
EOF
    helm install kafka bitnami/kafka -n i40-production -f /tmp/kafka-values.yaml --wait || echo "Kafka already installed"

    # Install PostgreSQL
    echo "Installing PostgreSQL..."
    cat > /tmp/postgresql-values.yaml <<EOF
auth:
  username: i40admin
  password: i40secret
  database: i40_production
primary:
  initdb:
    scripts:
      init.sql: |
$(cat docker/postgres/init.sql | sed 's/^/        /')
EOF
    helm install postgresql bitnami/postgresql -n i40-production -f /tmp/postgresql-values.yaml --wait || echo "PostgreSQL already installed"

    # Install Redis
    echo "Installing Redis..."
    helm install redis bitnami/redis -n i40-production --set auth.enabled=false --wait || echo "Redis already installed"

    # Install Prometheus
    echo "Installing Prometheus stack..."
    helm install prometheus prometheus-community/kube-prometheus-stack -n i40-production --wait || echo "Prometheus already installed"

    echo -e "${GREEN}✓ Kubernetes environment ready${NC}"
    echo ""

    # Port forward commands
    echo "To access services, run:"
    echo "  kubectl port-forward -n i40-production svc/grafana 3000:80"
    echo "  kubectl port-forward -n i40-production svc/prometheus-operated 9090:9090"
    echo ""
fi

# Step 4: Build services
echo "Step 4: Building services..."
make services-build || { echo -e "${RED}Failed to build services${NC}"; exit 1; }
echo -e "${GREEN}✓ Services built${NC}"
echo ""

# Step 5: Setup complete
echo "======================================"
echo -e "${GREEN}Setup Complete!${NC}"
echo "======================================"
echo ""
echo "Next steps:"
echo ""

if [[ "$MODE" == "local" || "$MODE" == "both" ]]; then
    echo "Local Development:"
    echo "  1. Start services: make services-up"
    echo "  2. View logs: make logs"
    echo "  3. Test: make test-integration"
    echo ""
fi

if [[ "$MODE" == "k8s" || "$MODE" == "both" ]]; then
    echo "Kubernetes Deployment:"
    echo "  1. Build images: make docker-build"
    echo "  2. Deploy: make k8s-deploy"
    echo "  3. Check status: make k8s-status"
    echo ""
fi

echo "Documentation:"
echo "  - README: README.md"
echo "  - Deployment Guide: DEPLOYMENT.md"
echo "  - Architecture Docs: docs/"
echo ""

echo "Happy building! 🚀"
