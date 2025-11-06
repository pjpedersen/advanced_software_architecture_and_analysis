module github.com/i40/production-system/services/go/release-policy

go 1.21

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/google/uuid v1.5.0
	github.com/i40/production-system/services/go/pkg v0.0.0
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/trace v1.21.0
)

replace github.com/i40/production-system/services/go/pkg => ../pkg
