// Package kafka builds franz-go clients from service configuration.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/omnomkar/event-driven-orders/pkg/health"
)

// Config holds the settings every Kafka client needs.
type Config struct {
	Brokers  []string
	ClientID string
}

// NewClient returns a franz-go client. It does not contact the brokers until
// first use, so a service can start before Kafka is up; readiness reports
// the gap. Extra options (consumer group, topics) are appended.
func NewClient(cfg Config, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return client, nil
}

// ReadyCheck returns a health check that passes when a broker answers.
func ReadyCheck(client *kgo.Client) health.CheckFunc {
	return func(ctx context.Context) error {
		if err := client.Ping(ctx); err != nil {
			return fmt.Errorf("ping kafka: %w", err)
		}
		return nil
	}
}
