# I4.0 Production System - Complete Deployment Guide

This guide will help you deploy the complete Industry 4.0 Production System with all components.

## Prerequisites

- Docker Desktop with Kubernetes enabled (or minikube)
- kubectl CLI tool
- Helm 3
- Go 1.21+
- Python 3.11+
- make
- protoc (Protocol Buffers compiler)

## Quick Start (Local Development)

### Step 1: Install Tools

```bash
# Install protobuf compiler
brew install protobuf  # macOS
# or
sudo apt-get install protobuf-compiler  # Linux

# Install Go tools
make install-tools
```

### Step 2: Generate Protobuf Code

```bash
# Generate all protobuf code
make proto
```

### Step 3: Start Infrastructure

```bash
# Start Kafka, PostgreSQL, Redis, etc.
make infra-up

# Wait for services to be ready (check with)
docker-compose -f docker/docker-compose.infra.yml ps

# View logs
make infra-logs
```

### Step 4: Build and Start Services

```bash
# Build all Go services
make services-build

# Start services locally
make services-up
```

### Step 5: Verify Deployment

Open in browser:
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686
- Kafka UI: http://localhost:8080
- Scheduler API: http://localhost:8090 (POST /api/v1/plans)

## Production Deployment (Kubernetes)

### Step 1: Setup Kubernetes Cluster

```bash
# For local testing with Docker Desktop
kubectl config use-context docker-desktop

# Create namespace
kubectl create namespace i40-production
```

### Step 2: Install Dependencies

```bash
# Install Kafka, PostgreSQL, Redis via Helm
make k8s-install-deps

# Wait for all pods to be ready
kubectl get pods -n i40-production --watch
```

### Step 3: Build and Push Docker Images

```bash
# Build all Docker images
make docker-build

# Tag for our registry (set registry URL)
export REGISTRY=registry.company.internal

# Push images
./scripts/docker-push.sh
```

### Step 4: Deploy Services

```bash
# Deploy all services
make k8s-deploy

# Check status
make k8s-status
```

### Step 5: Access Services

```bash
# Port forward to access services locally
make k8s-port-forward

# Or setup ingress (see k8s/ingress.yaml)
kubectl apply -f k8s/ingress.yaml
```

## Blue-Green Deployment

### Deploy New Version

```bash
# Deploy to blue environment
make k8s-deploy-blue

# Test blue environment
curl http://blue.i40-production.local/health

# Run canary tests
make test-canary

# Switch traffic to blue
make k8s-cutover-blue

# Monitor for 10 minutes, rollback if needed
make k8s-cutover-green  # Rollback
```

## Service Architecture

### Core Services

1. **Scheduler Service** (Port 8080)
   - Production planning
   - Resource allocation
   - Changeover coordination

2. **Device Registry** (Port 8181)
   - Device registration
   - Identity management
   - Firmware tracking

3. **PLC Adapters** (Ports 8082-8083)
   - PLC-P1: Injection molding
   - PLC-P2: Assembly line
   - Protocol translation

4. **Engraver Adapter** (Port 8084)
   - Template management
   - Engraving coordination

5. **Quality Service** (Port 8085)
   - Validation results
   - Disposition decisions

6. **Genealogy Service** (Port 8089)
   - Traceability
   - Recall queries

7. **Weather Aggregator** (Port 8087)
   - External API integration
   - Data correlation

### Supporting Services

- **Kafka** (Port 9092): Message bus
- **PostgreSQL** (Port 5432): Relational data
- **TimescaleDB** (Port 5433): Time-series data
- **Redis** (Port 6379): Caching
- **Vault** (Port 8200): Secrets management

## Testing

### Unit Tests

```bash
make test-unit
```

### Integration Tests

```bash
# Start infrastructure first
make infra-up

# Run integration tests
make test-integration
```

### End-to-End Tests

```bash
# Full system must be running
make services-up

# Run E2E tests
make test-e2e
```

### Load Tests

```bash
# Install k6
brew install k6

# Run load tests
make test-load
```

## Use Case Scenarios

### UC-1: Create Production Plan

```bash
curl -X POST http://localhost:8090/api/v1/plans \
  -H "Content-Type: application/json" \
  -d '{
    "batches": [
      {
        "batch_id": "B001",
        "product_sku": "WIC-001",
        "quantity": 100,
        "color": "BLACK",
        "start_time": "2025-01-20T08:00:00Z",
        "end_time": "2025-01-20T10:00:00Z",
        "priority": 1
      }
    ],
    "start_time": "2025-01-20T08:00:00Z",
    "end_time": "2025-01-20T18:00:00Z"
  }'
```

### UC-2: Register Device

```bash
curl -X POST http://localhost:8181/api/v1/devices \
  -H "Content-Type: application/json" \
  -d '{
    "pcb_id": "PCB-12345",
    "serial_number": "SN-12345",
    "firmware_version": "v1.0.0",
    "firmware_hash": "sha256:abc123..."
  }'
```

### UC-8: Query Recall

```bash
curl -X POST http://localhost:8089/api/v1/genealogy/recall/query \
  -H "Content-Type: application/json" \
  -d '{
    "firmware_versions": ["v1.0.0"],
    "colors": ["BLACK"],
    "production_date_from": "2025-01-01T00:00:00Z",
    "production_date_to": "2025-01-31T23:59:59Z"
  }'
```

## Monitoring & Observability

### Metrics (Prometheus)

Key metrics to monitor:
- `scheduler_plans_created_total`: Total production plans created
- `adapter_ack_latency_seconds`: ACK latency from adapters
- `device_registration_duration_seconds`: Device registration time
- `quality_disposition_total`: Total quality decisions
- `kafka_consumer_lag`: Consumer lag per topic

### Traces (Jaeger)

View distributed traces:
1. Go to http://localhost:16686
2. Select service: `scheduler-service`
3. Search for traces

### Logs (Stdout)

```bash
# All services
make logs

# Specific service
kubectl logs -n i40-production -l app=scheduler -f
```

### Dashboards (Grafana)

Pre-configured dashboards:
1. Production Overview
2. Service Health
3. Kafka Metrics
4. Database Performance
5. Quality Metrics

## Troubleshooting

### Kafka Connection Issues

```bash
# Check Kafka status
kubectl exec -n i40-production kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer groups
kubectl exec -n i40-production kafka-0 -- kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Database Connection Issues

```bash
# Check PostgreSQL
kubectl exec -n i40-production postgresql-0 -- psql -U i40admin -d i40_production -c "\dt"

# Check connections
kubectl exec -n i40-production postgresql-0 -- psql -U i40admin -d i40_production -c "SELECT * FROM pg_stat_activity;"
```

### Service Not Starting

```bash
# Check logs
kubectl logs -n i40-production <pod-name> --previous

# Check events
kubectl describe pod -n i40-production <pod-name>

# Check resource limits
kubectl top pods -n i40-production
```

## Security

### Vault Configuration

```bash
# Access Vault
export VAULT_ADDR=http://localhost:8200
export VAULT_TOKEN=root

# Enable PKI
vault secrets enable pki

# Generate root CA
vault write pki/root/generate/internal \
  common_name=i40-production.local \
  ttl=87600h
```

### mTLS Between Services

All service-to-service communication uses mTLS via Istio service mesh.

```bash
# Check certificate status
kubectl exec -n i40-production <pod-name> -c istio-proxy -- curl localhost:15000/certs
```

## Scaling

### Horizontal Pod Autoscaling

```bash
# Enable HPA for scheduler
kubectl autoscale deployment scheduler -n i40-production --cpu-percent=70 --min=2 --max=10

# Check HPA status
kubectl get hpa -n i40-production
```

### Kafka Partitions

Increase partitions for high-throughput topics:

```bash
kubectl exec -n i40-production kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --alter --topic production.plan --partitions 10
```

## Backup & Recovery

### Database Backup

```bash
kubectl exec -n i40-production postgresql-0 -- pg_dump -U i40admin i40_production > backup.sql
```

### Restore

```bash
kubectl exec -i -n i40-production postgresql-0 -- psql -U i40admin i40_production < backup.sql
```

## Performance Tuning

### Kafka

```yaml
# k8s/helm-values/kafka-values.yaml
replicaCount: 3
numPartitions: 10
defaultReplicationFactor: 3
```

### PostgreSQL

```yaml
# k8s/helm-values/postgresql-values.yaml
resources:
  limits:
    cpu: 4
    memory: 8Gi
postgresqlMaxConnections: 200
postgresqlSharedBuffers: 2GB
```

## Maintenance

### Rolling Updates

```bash
# Update image
kubectl set image deployment/scheduler -n i40-production scheduler=i40-scheduler:v2.0.0

# Check rollout status
kubectl rollout status deployment/scheduler -n i40-production

# Rollback if needed
kubectl rollout undo deployment/scheduler -n i40-production
```

### Cleanup

```bash
# Remove all resources
make clean-k8s

# Stop local infrastructure
make infra-down
```
