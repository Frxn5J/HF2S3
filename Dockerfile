# HF2S3 - production image (multi-stage, static binary, non-root at runtime)

# ---- Stage 1: build a static binary ------------------------------------------
# For reproducible builds pin these images by digest in your registry mirror.
FROM golang:1.24-alpine3.21 AS builder

WORKDIR /build

RUN apk add --no-cache ca-certificates git

# Cache the module download layer
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /build/hf2s3 ./cmd/hf2s3

# ---- Stage 2: minimal runtime ---------------------------------------------------
FROM alpine:3.21

# ca-certificates: HTTPS to Hugging Face. su-exec: drop root after fixing volume ownership.
RUN apk add --no-cache ca-certificates tzdata su-exec \
 && addgroup -S -g 10001 app \
 && adduser -S -u 10001 -G app -h /app app \
 && mkdir -p /data \
 && chown app:app /data

WORKDIR /app
COPY --from=builder /build/hf2s3 /app/hf2s3
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

ENV PORT=8080 \
    HF2S3_DB=/data/hf2s3_metadata.db

EXPOSE 8080

# Persistent metadata database and backups
VOLUME ["/data"]

# The readiness endpoint checks the database, not just that the process is up.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- "http://127.0.0.1:${PORT:-8080}/api/ready" >/dev/null || exit 1

# Starts as root only to make /data writable by the app user (volumes created by
# older releases are owned by root), then runs the gateway as uid 10001.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/app/hf2s3", "serve"]
