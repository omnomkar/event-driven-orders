package orders

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// outcomes returns the saga_outcomes_total value for outcome and reason.
func outcomes(m *SagaMetrics, outcome, reason string) float64 {
	return testutil.ToFloat64(m.outcomes.WithLabelValues(outcome, reason))
}

// durations returns how many saga_duration_seconds observations outcome has.
func durations(t *testing.T, m *SagaMetrics, outcome string) uint64 {
	t.Helper()
	var out dto.Metric
	if err := m.duration.WithLabelValues(outcome).(prometheus.Histogram).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetHistogram().GetSampleCount()
}

func TestSagaMetricsCountOnlyFinalTransitions(t *testing.T) {
	m := NewSagaMetrics(prometheus.NewRegistry())
	for _, tr := range []struct {
		e  Event
		to Status
	}{
		{EventStockReserved, StatusInventoryReserved}, // not final
		{EventPaymentSucceeded, StatusConfirmed},
		{EventPaymentFailed, StatusCancelling}, // not final: stock not back yet
		{EventStockReleased, StatusCancelled},
		{EventStockRejected, StatusCancelled},
		{EventPaymentSucceeded, StatusConfirmed},
	} {
		m.finished(tr.e, tr.to, 120*time.Millisecond)
	}
	m.DeadLettered()

	for _, c := range []struct {
		outcome, reason string
		want            float64
	}{
		{OutcomeConfirmed, ReasonNone, 2},
		{OutcomeCancelled, ReasonPaymentFailed, 1},
		{OutcomeCancelled, ReasonOutOfStock, 1},
		{OutcomeDeadLettered, ReasonNone, 1},
	} {
		if got := outcomes(m, c.outcome, c.reason); got != c.want {
			t.Errorf("saga_outcomes_total{%s,%s} = %v, want %v", c.outcome, c.reason, got, c.want)
		}
	}
	if got := durations(t, m, OutcomeConfirmed); got != 2 {
		t.Errorf("confirmed durations = %d, want 2", got)
	}
	if got := durations(t, m, OutcomeCancelled); got != 2 {
		t.Errorf("cancelled durations = %d, want 2", got)
	}
}

// Every series exists at zero before the first order, so the first
// increase is visible to rate() and to the metrics check script.
func TestSagaMetricsSeriesStartAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewSagaMetrics(reg)
	if n := testutil.CollectAndCount(reg, "saga_outcomes_total"); n != 4 {
		t.Errorf("saga_outcomes_total series = %d, want 4", n)
	}
	if n := testutil.CollectAndCount(reg, "saga_duration_seconds"); n != 2 {
		t.Errorf("saga_duration_seconds series = %d, want 2", n)
	}
}
