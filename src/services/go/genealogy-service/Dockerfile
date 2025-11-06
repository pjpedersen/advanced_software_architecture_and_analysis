# Multi-stage Dockerfile for genealogy-service

# Build stage
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /build

# Copy go workspace and modules
COPY go.work go.work.sum* ./
COPY services/go/pkg/go.mod services/go/pkg/go.sum services/go/pkg/
COPY services/go/genealogy-service/go.mod services/go/genealogy-service/go.sum* services/go/genealogy-service/

# Download dependencies
WORKDIR /build/services/go/genealogy-service
RUN go mod download

# Copy source code
COPY services/go/pkg /build/services/go/pkg
COPY services/go/genealogy-service /build/services/go/genealogy-service

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /app/server ./cmd/server

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

RUN addgroup -g 1000 app && \
    adduser -D -u 1000 -G app app

WORKDIR /app

COPY --from=builder /app/server /app/server

RUN chown -R app:app /app

USER app

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8089/health || exit 1

EXPOSE 8089

ENTRYPOINT ["/app/server"]
