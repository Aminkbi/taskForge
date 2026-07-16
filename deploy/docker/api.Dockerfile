FROM golang:1.25.0-alpine AS build
WORKDIR /src
ARG VERSION=dev
ARG COMMIT=unknown
ARG TARGETOS=linux
ARG TARGETARCH=amd64

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
  -trimpath \
  -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
  -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/aminkbi/taskforge" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
COPY --from=build /out/api /api
COPY --from=build /out/healthcheck /healthcheck
USER 65532:65532
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD ["/healthcheck"]
ENTRYPOINT ["/api"]
