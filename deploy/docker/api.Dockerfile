FROM golang:1.27.1-alpine@sha256:4cb7ac979db5fcc41cae44b2227ba5ab8a51e8807f40d9ba4dee20a0ad960b5b AS build
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

FROM gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2
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
