# Event-Driven Order Platform

Go microservices for order, payment, and inventory that coordinate through Apache Kafka using an orchestrated saga with a transactional outbox.

Status: early development. Setup instructions, the architecture diagram, and load test results will be added as the project grows.

## Layout

```
proto/       Protobuf definitions and generated Go code
services/    gateway, order, payment, inventory
pkg/         shared Go packages (config, logging, health, kafka)
deploy/      Docker Compose for local development, Helm charts
loadtest/    k6 scripts and saved results
docs/        design decisions
```

## Design decisions

See [docs/decisions.md](docs/decisions.md).
