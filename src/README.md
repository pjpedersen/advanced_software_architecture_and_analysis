# Industry 4.0 Production System - IoT Weather Information Center

Complete implementation of a heterogeneous Industry 4.0 production line for manufacturing customizable IoT weather displays.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        IT Layer (Cloud/Edge)                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │Scheduler │  │ Device   │  │ Quality  │  │Genealogy │       │
│  │ Service  │  │Registry  │  │ Service  │  │ Service  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
│       │             │             │             │               │
│       └─────────────┴─────────────┴─────────────┘               │
│                         │                                        │
│              ┌──────────▼──────────┐                            │
│              │   Kafka Message Bus  │                            │
│              │  (Schema Registry)   │                            │
│              └──────────┬──────────┘                            │
└─────────────────────────┼──────────────────────────────────────┘
                          │
┌─────────────────────────┼──────────────────────────────────────┐
│                    DMZ / Adapter Layer                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐               │
│  │PLC Adapter │  │PLC Adapter │  │  Engraver  │               │
│  │    P1      │  │    P2      │  │  Adapter   │               │
│  └──────┬─────┘  └──────┬─────┘  └──────┬─────┘               │
│         │               │                │                      │
│         └───────────────┴────────────────┘                      │
│                         │                                        │
│              ┌──────────▼──────────┐                            │
│              │   MQTT Broker (OT)   │                            │
│              └──────────┬──────────┘                            │
└─────────────────────────┼──────────────────────────────────────┘
                          │
┌─────────────────────────┼──────────────────────────────────────┐
│                    OT Layer (Shop Floor)                        │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│  │ PLC P1  │  │ PLC P2  │  │Engraver │  │Validation│          │
│  │(Molding)│  │(Assembly│  │Controller│  │ Station │          │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘          │
│      │             │             │             │                │
│  ┌───▼─────┐  ┌───▼─────┐  ┌───▼─────┐  ┌───▼─────┐          │
│  │ Robot   │  │Conveyor │  │Template │  │  Test   │          │
│  │  Arm    │  │  Belt   │  │Storage  │  │Fixtures │          │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
i4.0-production-system/
├── api/                          # API definitions
│   ├── proto/                    # Protobuf schemas
│   └── openapi/                  # REST API specs
├── services/                     # Microservices
│   ├── go/                       # Go services
│   │   ├── scheduler/
│   │   ├── plc-adapter-p1/
│   │   ├── plc-adapter-p2/
│   │   ├── engraver-adapter/
│   │   ├── hmi-service/
│   │   ├── flash-station/
│   │   ├── release-policy/
│   │   ├── device-registry/
│   │   ├── validation-controller/
│   │   ├── quality-service/
│   │   ├── template-service/
│   │   └── genealogy-service/
│   └── python/                   # Python services
│       ├── weather-aggregator/
│       └── data-quality/
├── simulators/                   # Hardware simulators
│   ├── plc-simulator/
│   ├── robot-simulator/
│   ├── engraver-simulator/
│   └── validation-station/
├── platform/                     # Platform services
│   ├── deployment-controller/
│   ├── schema-registry/
│   └── observability/
├── k8s/                          # Kubernetes manifests
│   ├── base/
│   ├── overlays/
│   └── blue-green/
├── docker/                       # Docker files
├── scripts/                      # Automation scripts
├── tests/                        # Integration tests
└── docs/                         # Documentation
```

## Quick Start

### Prerequisites
- Docker Desktop with Kubernetes enabled (or minikube)
- kubectl
- Helm 3
- Go 1.21+
- Python 3.11+
- make

### Local Development (Docker Compose)

```bash
# Start infrastructure
make infra-up

# Start services
make services-up

# View logs
make logs

# Stop everything
make down
```

### Kubernetes Deployment

```bash
# Install dependencies (Kafka, PostgreSQL, etc.)
make k8s-install-deps

# Deploy all services
make k8s-deploy

# Check status
kubectl get pods -n i40-production

# Access UI
make k8s-port-forward
```



## Testing

```bash
# Unit tests
make test-unit

# Integration tests
make test-integration

# E2E tests
make test-e2e

# Load tests
make test-load

# Chaos tests
make test-chaos
```

## Monitoring

Access monitoring dashboards:
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Jaeger: http://localhost:16686
- Kibana: http://localhost:5601

