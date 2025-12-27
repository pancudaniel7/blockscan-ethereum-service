# syntax=docker/dockerfile:1

# Builder stage: use official Go Alpine image matching go.mod version
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Install build dependencies
RUN apk add --no-cache git ca-certificates build-base

# Cache modules separately
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY pkg .

# Build a static linux binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w -extldflags=-static" -o /out/app ./cmd

############################################################
# Runtime stage: minimal Alpine with certs and non-root user
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -g '' appuser \
    && mkdir -p /app

WORKDIR /app

# Copy binary and configs
COPY --from=builder /out/app /usr/local/bin/app
COPY configs/ ./configs/

# Default environment (can be overridden at run time)
ENV GO_ENV=local

# Service port
EXPOSE 8081

USER appuser

ENTRYPOINT ["/usr/local/bin/app"]

