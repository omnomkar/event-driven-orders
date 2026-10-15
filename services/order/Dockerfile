# Build from the repository root so the replace directives in go.mod can
# reach pkg/ and proto/gen/go:
#   docker build -f services/order/Dockerfile .

FROM golang:1.27.2-bookworm AS build
WORKDIR /src
# go.work is for editors only; build from this module's own go.mod.
ENV GOWORK=off CGO_ENABLED=0

# Download dependencies in their own layer so code changes do not refetch them.
COPY pkg/go.mod pkg/go.sum pkg/
COPY proto/gen/go/go.mod proto/gen/go/go.sum proto/gen/go/
COPY services/order/go.mod services/order/go.sum services/order/
RUN cd services/order && go mod download

COPY pkg/ pkg/
COPY proto/gen/go/ proto/gen/go/
COPY services/order/ services/order/
RUN cd services/order && go build -trimpath -ldflags="-s -w" -o /out/order ./cmd/order

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/order /order
USER nonroot:nonroot
EXPOSE 9001 8081
ENTRYPOINT ["/order"]
