# HF2S3: Production Dockerfile for Coolify
# Multi-stage lightweight build (<30MB image)

# Stage 1: Build the Go binary statically
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Install CA certificates and git for dependencies
RUN apk add --no-cache ca-certificates git

# Cache Go modules layer
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Compile static binaries for Linux amd64
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -extldflags '-static'" -o /build/hf2s3 ./cmd/hf2s3
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -extldflags '-static'" -o /build/benchmark ./cmd/benchmark

# Stage 2: Minimal runtime image
FROM alpine:3.20

# Install ca-certificates (vital for HTTPS calls to Hugging Face) and tzdata
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/hf2s3 /app/hf2s3
COPY --from=builder /build/benchmark /app/benchmark

# Create persistent data directory for SQLite metadata
RUN mkdir -p /data

# Default environment variables
ENV PORT=8080 \
    HF2S3_DB=/data/hf2s3_metadata.db \
    ADMIN_USERNAME=admin \
    ADMIN_PASSWORD=admin123 \
    HF2S3_ACCESS_KEY=hf2s3-access-key \
    HF2S3_SECRET_KEY=hf2s3-secret-key \
    HF2S3_MASTER_KEY=hf2s3-aes-master-passphrase-2026 \
    HF2S3_CHUNK_SIZE_MB=32 \
    HF2S3_REGION=us-east-1

# Expose HTTP port (S3 Gateway & Web Console)
EXPOSE 8080

# Declare persistent volume mount point
VOLUME ["/data"]

# Coolify / Docker health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://localhost:${PORT:-8080}/api/health || exit 1

# Run the HF2S3 Gateway
ENTRYPOINT ["/app/hf2s3"]
