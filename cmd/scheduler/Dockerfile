# Scheduler image. Distroless runtime: the scheduler is a pure HTTP/Postgres
# binary with no need for a shell.

FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary, no cgo, stripped, reproducible build paths.
ENV CGO_ENABLED=0 GOOS=linux
RUN go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/scheduler \
    ./cmd/scheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/scheduler /scheduler
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/scheduler"]
