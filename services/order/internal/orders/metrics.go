package orders

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Saga outcome label values.
const (
	OutcomeConfirmed    = "confirmed"
	OutcomeCancelled    = "cancelled"
	OutcomeDeadLettered = "dead_lettered"

	ReasonNone          = "none"
	ReasonOutOfStock    = "out_of_stock"
	ReasonPaymentFailed = "payment_failed"
)

// SagaMetrics counts how sagas end and how long they take.
type SagaMetrics struct {
	outcomes *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewSagaMetrics creates the saga metrics and registers them with reg.
// Every label combination starts at zero, so a dashboard or rate() query
// sees the series before the first order of each kind.
func NewSagaMetrics(reg prometheus.Registerer) *SagaMetrics {
	m := &SagaMetrics{
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "saga_outcomes_total",
			Help: "Sagas that reached a final state, and saga replies dead-lettered by the orchestrator.",
		}, []string{"outcome", "reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "saga_duration_seconds",
			Help: "Time from order creation to CONFIRMED or CANCELLED, measured by the database clock.",
			// From tens of milliseconds (idle stack) up to a minute (backlog,
			// or a consumer restarting after a crash).
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"outcome"}),
	}
	reg.MustRegister(m.outcomes, m.duration)
	m.outcomes.WithLabelValues(OutcomeConfirmed, ReasonNone)
	m.outcomes.WithLabelValues(OutcomeCancelled, ReasonOutOfStock)
	m.outcomes.WithLabelValues(OutcomeCancelled, ReasonPaymentFailed)
	m.outcomes.WithLabelValues(OutcomeDeadLettered, ReasonNone)
	m.duration.WithLabelValues(OutcomeConfirmed)
	m.duration.WithLabelValues(OutcomeCancelled)
	return m
}

// finished records a committed transition. Only transitions into a final
// status count; e is the event that caused it, which says why a cancelled
// order was cancelled.
func (m *SagaMetrics) finished(e Event, to Status, elapsed time.Duration) {
	outcome, reason, ok := classify(e, to)
	if !ok {
		return
	}
	m.outcomes.WithLabelValues(outcome, reason).Inc()
	m.duration.WithLabelValues(outcome).Observe(elapsed.Seconds())
}

// DeadLettered counts a saga reply the orchestrator gave up on. The order it
// belongs to is stuck where it was until someone replays the record.
func (m *SagaMetrics) DeadLettered() {
	m.outcomes.WithLabelValues(OutcomeDeadLettered, ReasonNone).Inc()
}

// classify maps a transition to its outcome labels, and false if the
// transition is not into a final status. Any inventory rejection (no stock,
// unknown product, invalid line) counts as out_of_stock: it is the "nothing
// could be reserved" path, with no payment attempt.
func classify(e Event, to Status) (outcome, reason string, ok bool) {
	switch {
	case to == StatusConfirmed:
		return OutcomeConfirmed, ReasonNone, true
	case to == StatusCancelled && e == EventStockRejected:
		return OutcomeCancelled, ReasonOutOfStock, true
	case to == StatusCancelled && e == EventStockReleased:
		return OutcomeCancelled, ReasonPaymentFailed, true
	}
	return "", "", false
}
