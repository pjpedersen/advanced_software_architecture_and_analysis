module github.com/i40/production-system/services/scheduler

go 1.21

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/google/uuid v1.5.0
	github.com/gorilla/mux v1.8.1
	github.com/lib/pq v1.10.9
	github.com/prometheus/client_golang v1.17.0
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/sdk v1.21.0
	go.opentelemetry.io/otel/trace v1.21.0
	google.golang.org/protobuf v1.31.0
)
