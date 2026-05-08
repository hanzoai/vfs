FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.version=${VERSION}" \
    -o /vfs ./cmd/vfs

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /vfs /usr/local/bin/vfs
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/vfs"]
