# Build stage: compiles the binary. Nothing here ships.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Copy the module files alone first. Docker caches each layer, so dependencies
# are only re-downloaded when go.mod or go.sum change — not on every code edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a statically linked binary with no libc dependency,
# so it runs on any base image regardless of what's installed there.
RUN CGO_ENABLED=0 go build -o /taskqueue .

# Runtime stage: starts clean, so the Go toolchain and source never ship.
# The final image is a few megabytes instead of several hundred.
FROM alpine:3.20
WORKDIR /app

COPY --from=build /taskqueue /app/taskqueue

# The status page is served off disk, so it has to come along.
COPY static ./static

# Documentation only — Compose does the actual port publishing.
EXPOSE 8080

CMD ["/app/taskqueue"]