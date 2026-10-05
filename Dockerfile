# syntax=docker/dockerfile:1

# Build stage: always runs on the build machine's architecture and
# cross-compiles for the target platform, so multi-arch builds don't need
# emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm@sha256:69a7b9788769bec032d238959b61854e9ae87f57be9029ec04e9885fabf99195 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=""
ARG DATE=""
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /out/node ./cmd/node \
 && mkdir -p /out/data

# Runtime stage: distroless static image (no shell, no package manager),
# running as the unprivileged "nonroot" user (65532).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

LABEL org.opencontainers.image.title="p2p-model-distribution" \
      org.opencontainers.image.description="Peer-to-peer distribution of large ML model files over an authenticated Kademlia DHT" \
      org.opencontainers.image.source="https://github.com/IEEECS-VIT/p2p-model-distribution" \
      org.opencontainers.image.licenses="MIT"

COPY --from=build /out/node /usr/local/bin/node
# /data holds node.key (the node's identity), chunks and manifests. It is
# owned by the runtime user so the key can be created with mode 0600.
COPY --from=build --chown=65532:65532 /out/data /data

USER 65532:65532
VOLUME ["/data"]
EXPOSE 9000/tcp

# Defaults live in the entrypoint so extra arguments are appended rather
# than replacing them (a later -data/-listen overrides these).
ENTRYPOINT ["/usr/local/bin/node", "-data", "/data", "-listen", "0.0.0.0:9000"]
