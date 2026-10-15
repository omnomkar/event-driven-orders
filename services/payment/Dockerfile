# Build from the repository root so the replace directives in go.mod can
# reach pkg/ and proto/gen/go:
#   docker build -f services/payment/Dockerfile .

FROM golang:1.27.2-bookworm AS build
WORKDIR /src
# go.work is for editors only; build from this module's own go.mod.
ENV GOWORK=off CGO_ENABLED=0

# Download dependencies in their own layer so code changes do not refetch them.
COPY pkg/go.mod pkg/go.sum pkg/
COPY proto/gen/go/go.mod proto/gen/go/go.sum proto/gen/go/
COPY services/payment/go.mod services/payment/go.sum services/payment/
RUN cd services/payment && go mod download

COPY pkg/ pkg/
COPY proto/gen/go/ proto/gen/go/
COPY services/payment/ services/payment/
RUN cd services/payment && go build -trimpath -ldflags="-s -w" -o /out/payment ./cmd/payment

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payment /payment
USER nonroot:nonroot
EXPOSE 9002 8082
ENTRYPOINT ["/payment"]
