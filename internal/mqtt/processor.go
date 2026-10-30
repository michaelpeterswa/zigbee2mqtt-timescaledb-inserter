package mqtt

import (
	"context"
	"log/slog"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/metrics"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/timescale"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/zigbee"
)

const (
	subscribeQoS = 1
	writeTimeout = 5 * time.Second
)

// Processor turns zigbee2mqtt messages into stored readings.
type Processor struct {
	registry *zigbee.Registry
	writer   timescale.Writer
	metrics  *metrics.Metrics
	now      func() time.Time
}

func NewProcessor(reg *zigbee.Registry, w timescale.Writer, m *metrics.Metrics, now func() time.Time) *Processor {
	return &Processor{registry: reg, writer: w, metrics: m, now: now}
}

// Subscribe subscribes to everything under the base topic. Pass it to
// NewMQTTClient as the onConnect callback; nothing else should subscribe.
func (p *Processor) Subscribe(c paho.Client) {
	topic := p.registry.BaseTopic() + "/#"
	token := c.Subscribe(topic, subscribeQoS, func(_ paho.Client, msg paho.Message) {
		p.Handle(msg.Topic(), msg.Payload())
	})
	token.Wait()
	if err := token.Error(); err != nil {
		slog.Error("MQTT subscribe failed", slog.String("topic", topic), slog.String("error", err.Error()))
		return
	}
	slog.Info("MQTT subscribed", slog.String("topic", topic))
}

// Handle processes one message. It never panics and never returns an error:
// anything that cannot be stored is logged, counted and dropped.
func (p *Processor) Handle(topic string, payload []byte) {
	ctx := context.Background()
	route, device := p.registry.Route(topic)

	switch route {
	case zigbee.RouteBridgeDevices:
		if err := p.registry.SetDevices(payload); err != nil {
			slog.Error("ignoring bad bridge/devices", slog.String("error", err.Error()))
		}
	case zigbee.RouteBridgeGroups:
		if err := p.registry.SetGroups(payload); err != nil {
			slog.Error("ignoring bad bridge/groups", slog.String("error", err.Error()))
		}
	case zigbee.RouteUnknown:
		p.metrics.Received(ctx)
		p.metrics.Dropped(ctx, metrics.ReasonUnknownDevice)
		slog.Warn("dropping message from unknown device", slog.String("topic", topic))
	case zigbee.RouteDevice:
		p.metrics.Received(ctx)
		p.store(ctx, topic, device, payload)
	case zigbee.RouteGroup, zigbee.RouteIgnore:
	}
}

func (p *Processor) store(ctx context.Context, topic string, device zigbee.Device, payload []byte) {
	readings, err := zigbee.Flatten(payload)
	if err != nil {
		p.metrics.Dropped(ctx, metrics.ReasonParseError)
		slog.Warn("dropping unparseable message", slog.String("topic", topic), slog.String("error", err.Error()))
		return
	}
	if len(readings) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	start := time.Now()
	if err := p.writer.WriteReadings(ctx, p.now(), device, readings); err != nil {
		p.metrics.Dropped(ctx, metrics.ReasonDBError)
		slog.Error("dropping message, insert failed",
			slog.String("device", device.Name), slog.String("error", err.Error()))
		return
	}
	p.metrics.Inserted(ctx, len(readings), time.Since(start))
	slog.Debug("stored readings", slog.String("device", device.Name), slog.Int("rows", len(readings)))
}
