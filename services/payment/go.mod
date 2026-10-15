module github.com/omnomkar/event-driven-orders/services/payment

go 1.27.2

replace (
	github.com/omnomkar/event-driven-orders/pkg => ../../pkg
	github.com/omnomkar/event-driven-orders/proto/gen/go => ../../proto/gen/go
)

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/omnomkar/event-driven-orders/pkg v0.0.0-00010101000000-000000000000
	github.com/omnomkar/event-driven-orders/proto/gen/go v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.24.0
	google.golang.org/grpc v1.84.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/twmb/franz-go v1.22.2 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
