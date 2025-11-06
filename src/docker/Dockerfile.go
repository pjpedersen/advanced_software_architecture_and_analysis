# Multi-stage Dockerfile for Go services

# Build stage
FROM golang:1.21-alpine AS builder

# Install dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.work go.work.sum* ./
COPY services/go/pkg/go.mod services/go/pkg/go.sum services/go/pkg/

# Build argument for which service to build
ARG SERVICE

# Copy service-specific files
COPY services/go/${SERVICE} services/go/${SERVICE}/

# Download dependencies
WORKDIR /build/services/go/${SERVICE}
RUN go mod download

# Copy shared libraries
COPY services/go/pkg /build/services/go/pkg

# Build the service
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /app/server ./cmd/server

# Runtime stage
FROM alpine:3.19

# Install CA certificates for HTTPS
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 app && \
    adduser -D -u 1000 -G app app

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/server /app/server

# Set ownership
RUN chown -R app:app /app

# Switch to non-root user
USER app

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/server", "health"] || exit 1

# Expose port
EXPOSE 8080

# Run the service
ENTRYPOINT ["/app/server"]
