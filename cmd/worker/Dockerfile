# Worker image. Alpine runtime instead of distroless so `kubectl exec` is
# usable for debugging during the demo, and so a future worker that shells
# out to user task commands has the basic tools available.

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0 GOOS=linux
RUN go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/worker \
    ./cmd/worker

FROM alpine:3.20
RUN adduser -D -u 10001 workron
COPY --from=builder /out/worker /worker
USER 10001
ENTRYPOINT ["/worker"]
