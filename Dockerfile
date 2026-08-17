# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /nexus-server ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /nexus

COPY --from=builder /nexus-server /nexus/nexus-server

EXPOSE 8080

VOLUME ["/nexus/data"]

ENTRYPOINT ["/nexus/nexus-server", "-http-addr=:8080", "-data-dir=/nexus/data"]
