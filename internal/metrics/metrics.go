package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Reasons a message is dropped.
const (
	ReasonUnknownDevice = "unknown_device"
	ReasonParseError    = "parse_error"
	ReasonDBError       = "db_error"
)

// Metrics are the inserter's own instruments. With the Prometheus exporter
// the counters get a _total suffix and the histogram a _seconds suffix.
type Metrics struct {
	received metric.Int64Counter
	inserted metric.Int64Counter
	dropped  metric.Int64Counter
	insert   metric.Float64Histogram
}

func New(meter metric.Meter) (*Metrics, error) {
	received, err := meter.Int64Counter("zigbee_messages_received",
		metric.WithDescription("Device state messages received from zigbee2mqtt."))
	if err != nil {
		return nil, fmt.Errorf("create received counter: %w", err)
	}

	inserted, err := meter.Int64Counter("zigbee_rows_inserted",
		metric.WithDescription("Rows inserted into sensors.zigbee."))
	if err != nil {
		return nil, fmt.Errorf("create inserted counter: %w", err)
	}

	dropped, err := meter.Int64Counter("zigbee_messages_dropped",
		metric.WithDescription("Device messages not stored, by reason."))
	if err != nil {
		return nil, fmt.Errorf("create dropped counter: %w", err)
	}

	insert, err := meter.Float64Histogram("zigbee_insert_duration",
		metric.WithUnit("s"),
		metric.WithDescription("Time to insert one message's readings."))
	if err != nil {
		return nil, fmt.Errorf("create insert histogram: %w", err)
	}

	return &Metrics{received: received, inserted: inserted, dropped: dropped, insert: insert}, nil
}

func (m *Metrics) Received(ctx context.Context) { m.received.Add(ctx, 1) }

func (m *Metrics) Inserted(ctx context.Context, rows int, took time.Duration) {
	m.inserted.Add(ctx, int64(rows))
	m.insert.Record(ctx, took.Seconds())
}

func (m *Metrics) Dropped(ctx context.Context, reason string) {
	m.dropped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
