package timescale

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/zigbee"
)

// The integration tests need a TimescaleDB with the sensors schema applied
// (see `make testdb`). They are skipped when TIMESCALE_TEST_CONN_STRING is
// unset.
func testClient(t *testing.T) *TimescaleClient {
	t.Helper()
	conn := os.Getenv("TIMESCALE_TEST_CONN_STRING")
	if conn == "" {
		t.Skip("TIMESCALE_TEST_CONN_STRING not set")
	}

	ctx := context.Background()
	c, err := NewTimescaleClient(ctx, conn)
	if err != nil {
		t.Fatalf("NewTimescaleClient: %v", err)
	}
	t.Cleanup(c.Close)

	if _, err := c.Pool.Exec(ctx, "TRUNCATE sensors.zigbee, sensors.zigbee_metric"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return c
}

func num(v float64) *float64 { return &v }
func str(s string) *string   { return &s }

func TestWriteReadings(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()

	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	device := zigbee.Device{IEEE: "0x00124b0012345678", Name: "office"}
	readings := []zigbee.Reading{
		{Metric: "humidity", Value: num(51.72)},
		{Metric: "temperature", Value: num(25.81)},
		{Metric: "action", TextValue: str("single")},
	}

	if err := c.WriteReadings(ctx, at, device, readings); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	rows, err := c.Pool.Query(ctx, `
		SELECT time, device_ieee, device_name, metric, value, text_value
		FROM sensors.zigbee ORDER BY metric`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		time               time.Time
		ieee, name, metric string
		value              *float64
		textValue          *string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.time, &r.ieee, &r.name, &r.metric, &r.value, &r.textValue); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	for _, r := range got {
		if !r.time.Equal(at) || r.ieee != device.IEEE || r.name != device.Name {
			t.Errorf("row %s: got (%v, %s, %s), want (%v, %s, %s)", r.metric, r.time, r.ieee, r.name, at, device.IEEE, device.Name)
		}
	}
	if got[0].metric != "action" || got[0].value != nil || got[0].textValue == nil || *got[0].textValue != "single" {
		t.Errorf("action row: value=%v text_value=%v, want nil/single", got[0].value, got[0].textValue)
	}
	if got[1].metric != "humidity" || got[1].value == nil || *got[1].value != 51.72 || got[1].textValue != nil {
		t.Errorf("humidity row: value=%v text_value=%v, want 51.72/nil", got[1].value, got[1].textValue)
	}
}

func TestWriteReadingsRejectsEmptyReading(t *testing.T) {
	// No database needed: the reading is rejected before any query.
	c := &TimescaleClient{}

	err := c.WriteReadings(context.Background(), time.Now(), zigbee.Device{IEEE: "0x01", Name: "x"},
		[]zigbee.Reading{{Metric: "broken"}})
	if err == nil {
		t.Fatal("WriteReadings with neither value nor text_value: expected error")
	}
}

func TestUpsertUnits(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()

	first := []zigbee.MetricUnit{
		{IEEE: "0x00124b0012345678", Metric: "humidity", Unit: "%"},
		{IEEE: "0x00124b0012345678", Metric: "temperature", Unit: "°C"},
	}
	if err := c.UpsertUnits(ctx, first); err != nil {
		t.Fatalf("UpsertUnits(first): %v", err)
	}
	// A re-published device list with a changed unit replaces the old one.
	second := []zigbee.MetricUnit{{IEEE: "0x00124b0012345678", Metric: "temperature", Unit: "°F"}}
	if err := c.UpsertUnits(ctx, second); err != nil {
		t.Fatalf("UpsertUnits(second): %v", err)
	}
	if err := c.UpsertUnits(ctx, nil); err != nil {
		t.Fatalf("UpsertUnits(nil): %v", err)
	}

	rows, err := c.Pool.Query(ctx, "SELECT metric, unit FROM sensors.zigbee_metric ORDER BY metric")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var metric, unit string
		if err := rows.Scan(&metric, &unit); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[metric] = unit
	}
	want := map[string]string{"humidity": "%", "temperature": "°F"}
	if len(got) != len(want) || got["humidity"] != "%" || got["temperature"] != "°F" {
		t.Errorf("units = %v, want %v", got, want)
	}
}
