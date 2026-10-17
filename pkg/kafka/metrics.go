package kafka

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Package-level metrics, registered once with the default Prometheus
// registry (promauto registers them as they are created). Every service
// that uses this package exposes them on /metrics.
var (
	dlqRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_dlq_records_total",
		Help: "Records published to a dead-letter topic, by the topic they failed on.",
	}, []string{"source_topic"})

	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Records not yet committed by the consumer group: partition end offset minus committed offset.",
	}, []string{"group", "topic", "partition"})
)

// DefaultLagInterval is how often RunLagGauge refreshes kafka_consumer_lag.
const DefaultLagInterval = 10 * time.Second

// RunLagGauge sets kafka_consumer_lag for group every interval until ctx is
// cancelled. client can be any client connected to the cluster; the lag is
// read with admin requests (the group's committed offsets and each
// partition's end offset), so it measures what the group has committed, not
// what it has fetched. Errors are logged and the gauge keeps its last value.
func RunLagGauge(ctx context.Context, client *kgo.Client, group string, interval time.Duration, logger *slog.Logger) {
	adm := kadm.NewClient(client)
	logger = logger.With("component", "lag_gauge", "group", group)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := updateLag(ctx, adm, group); err != nil && ctx.Err() == nil {
			logger.Warn("read consumer lag failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func updateLag(ctx context.Context, adm *kadm.Client, group string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lags, err := adm.Lag(ctx, group)
	if err != nil {
		return err
	}
	described, ok := lags[group]
	if !ok {
		return nil
	}
	if err := described.Error(); err != nil {
		return err
	}
	for _, l := range described.Lag.Sorted() {
		if l.Err != nil || l.Lag < 0 {
			continue // unknown for now; keep the last value
		}
		consumerLag.WithLabelValues(group, l.Topic, strconv.Itoa(int(l.Partition))).Set(float64(l.Lag))
	}
	return nil
}
