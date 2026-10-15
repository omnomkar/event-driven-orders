# Build from the repository root so the replace directives in go.mod can
# reach pkg/ and proto/gen/go:
#   docker build -f services/inventory/Dockerfile .

FROM golang:1.27.2-bookworm AS build
WORKDIR /src
# go.work is for editors only; build from this module's own go.mod.
ENV GOWORK=off CGO_ENABLED=0

# Download dependencies in their own layer so code changes do not refetch them.
COPY pkg/go.mod pkg/go.sum pkg/
COPY proto/gen/go/go.mod proto/gen/go/go.sum proto/gen/go/
COPY services/inventory/go.mod services/inventory/go.sum services/inventory/
RUN cd services/inventory && go mod download

COPY pkg/ pkg/
COPY proto/gen/go/ proto/gen/go/
COPY services/inventory/ services/inventory/
RUN cd services/inventory && go build -trimpath -ldflags="-s -w" -o /out/inventory ./cmd/inventory

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/inventory /inventory
USER nonroot:nonroot
EXPOSE 9003 8083
ENTRYPOINT ["/inventory"]
