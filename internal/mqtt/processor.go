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
	// writeQueueSize bounds how many messages wait for the database. Handle
	// runs on paho's router goroutine and must never block on the database
	// (that stalls the network loop until the broker connection times out),
	// so writes happen on one writer goroutine and overflow is dropped.
	writeQueueSize = 64
)

// writeJob is either one message's readings or, when units is set, the
// metric units from a bridge/devices publish.
type writeJob struct {
	at       time.Time
	device   zigbee.Device
	readings []zigbee.Reading
	units    []zigbee.MetricUnit
}

// Processor turns zigbee2mqtt messages into stored readings.
type Processor struct {
	registry *zigbee.Registry
	writer   timescale.Writer
	metrics  *metrics.Metrics
	now      func() time.Time

	queue chan writeJob
	done  chan struct{}
}

// NewProcessor starts the writer goroutine; call Close to stop it.
func NewProcessor(reg *zigbee.Registry, w timescale.Writer, m *metrics.Metrics, now func() time.Time) *Processor {
	p := &Processor{
		registry: reg,
		writer:   w,
		metrics:  m,
		now:      now,
		queue:    make(chan writeJob, writeQueueSize),
		done:     make(chan struct{}),
	}
	go p.writeLoop()
	return p
}

// Close stops accepting writes and waits for queued ones to finish. Call it
// after the MQTT client has disconnected, so Handle is no longer called.
func (p *Processor) Close() {
	close(p.queue)
	<-p.done
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
			return
		}
		p.storeUnits(payload)
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

	select {
	case p.queue <- writeJob{at: p.now(), device: device, readings: readings}:
	default:
		p.metrics.Dropped(ctx, metrics.ReasonQueueFull)
		slog.Error("dropping message, write queue full", slog.String("device", device.Name))
	}
}

func (p *Processor) storeUnits(payload []byte) {
	units, err := zigbee.ParseUnits(payload)
	if err != nil {
		slog.Error("ignoring units in bad bridge/devices", slog.String("error", err.Error()))
		return
	}
	if len(units) == 0 {
		return
	}

	select {
	case p.queue <- writeJob{units: units}:
	default:
		slog.Error("dropping metric units, write queue full", slog.Int("units", len(units)))
	}
}

func (p *Processor) writeLoop() {
	defer close(p.done)
	for job := range p.queue {
		p.write(job)
	}
}

func (p *Processor) write(job writeJob) {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	if job.units != nil {
		// Units are re-sent on every bridge/devices publish (including each
		// reconnect), so a failed upsert is only logged.
		if err := p.writer.UpsertUnits(ctx, job.units); err != nil {
			slog.Error("metric units upsert failed", slog.String("error", err.Error()))
			return
		}
		slog.Debug("stored metric units", slog.Int("units", len(job.units)))
		return
	}

	start := time.Now()
	if err := p.writer.WriteReadings(ctx, job.at, job.device, job.readings); err != nil {
		p.metrics.Dropped(ctx, metrics.ReasonDBError)
		slog.Error("dropping message, insert failed",
			slog.String("device", job.device.Name), slog.String("error", err.Error()))
		return
	}
	p.metrics.Inserted(ctx, len(job.readings), time.Since(start))
	slog.Debug("stored readings", slog.String("device", job.device.Name), slog.Int("rows", len(job.readings)))
}
