FROM golang:1.26.4-alpine AS builder
RUN apk add --no-cache ca-certificates tzdata
RUN addgroup -g 65532 -S nonroot && adduser -u 65532 -S nonroot -G nonroot
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.version=${VERSION}" \
    -o /vfs ./cmd/vfs

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /vfs /usr/local/bin/vfs
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/vfs"]
