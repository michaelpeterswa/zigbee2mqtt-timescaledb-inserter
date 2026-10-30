package timescale

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/zigbee"
)

var (
	zigbeeTable   = pgx.Identifier{"sensors", "zigbee"}
	zigbeeColumns = []string{"time", "device_ieee", "device_name", "metric", "value", "text_value"}
)

// Writer stores the readings from one device message.
type Writer interface {
	WriteReadings(ctx context.Context, t time.Time, d zigbee.Device, rs []zigbee.Reading) error
}

type TimescaleClient struct {
	Pool *pgxpool.Pool
}

func NewTimescaleClient(ctx context.Context, connString string) (*TimescaleClient, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	cfg.ConnConfig.Tracer = otelpgx.NewTracer()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &TimescaleClient{Pool: pool}, nil
}

func (c *TimescaleClient) Close() {
	c.Pool.Close()
}

// WriteReadings inserts all readings from one message in a single COPY, so a
// message is stored entirely or not at all.
func (c *TimescaleClient) WriteReadings(ctx context.Context, t time.Time, d zigbee.Device, rs []zigbee.Reading) error {
	rows := make([][]any, 0, len(rs))
	for _, r := range rs {
		if (r.Value == nil) == (r.TextValue == nil) {
			return fmt.Errorf("reading %q: exactly one of value and text_value must be set", r.Metric)
		}
		rows = append(rows, []any{t, d.IEEE, d.Name, r.Metric, r.Value, r.TextValue})
	}
	if len(rows) == 0 {
		return nil
	}

	_, err := c.Pool.CopyFrom(ctx, zigbeeTable, zigbeeColumns, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy readings: %w", err)
	}
	return nil
}
