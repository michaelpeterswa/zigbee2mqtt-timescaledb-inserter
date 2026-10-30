package metrics

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Inserts take milliseconds; the histogram is in seconds, so OTel's default
// buckets (0, 5, 10, ... 10000) would put every insert in one bucket.
func TestInsertDurationBucketsAreSubSecond(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	m, err := New(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	m.Inserted(context.Background(), 4, 3*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "zigbee_insert_duration" {
				continue
			}
			dp := md.Data.(metricdata.Histogram[float64]).DataPoints[0]
			if dp.Bounds[0] > 0.001 {
				t.Errorf("first bucket bound = %v, want <= 0.001s", dp.Bounds[0])
			}
			if dp.Bounds[len(dp.Bounds)-1] > 10 {
				t.Errorf("last bucket bound = %v, want <= 10s", dp.Bounds[len(dp.Bounds)-1])
			}
			return
		}
	}
	t.Fatal("zigbee_insert_duration not collected")
}
