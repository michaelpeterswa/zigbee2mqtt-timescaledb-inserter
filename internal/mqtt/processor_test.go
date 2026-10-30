package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/metrics"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/zigbee"
)

const (
	testClientID = "inserter-under-test"
	devicesJSON  = `[{"ieee_address":"0x01","friendly_name":"office","type":"EndDevice"},
	                 {"ieee_address":"0x02","friendly_name":"kitchen/temp","type":"EndDevice"}]`
	groupsJSON = `[{"id":1,"friendly_name":"downstairs","members":[]}]`
	sensorJSON = `{"battery":100,"humidity":51.72,"linkquality":112,"temperature":25.81,"humidity_calibration":0}`
)

var fixedTime = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type write struct {
	at       time.Time
	device   zigbee.Device
	readings []zigbee.Reading
}

// fakeWriter records writes instead of touching a database.
type fakeWriter struct {
	mu     sync.Mutex
	writes []write
	units  [][]zigbee.MetricUnit
	err    error
}

func (f *fakeWriter) UpsertUnits(_ context.Context, units []zigbee.MetricUnit) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.units = append(f.units, units)
	return f.err
}

func (f *fakeWriter) allUnits() [][]zigbee.MetricUnit {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]zigbee.MetricUnit(nil), f.units...)
}

func (f *fakeWriter) WriteReadings(_ context.Context, t time.Time, d zigbee.Device, rs []zigbee.Reading) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, write{at: t, device: d, readings: rs})
	return f.err
}

func (f *fakeWriter) all() []write {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]write(nil), f.writes...)
}

type harness struct {
	t         *testing.T
	broker    *mqttserver.Server
	writer    *fakeWriter
	reader    *sdkmetric.ManualReader
	client    *MQTTClient
	connects  atomic.Int32
	processor *Processor
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// newHarness starts an embedded broker, publishes the given retained
// messages, and connects a real paho client wired to a Processor.
func newHarness(t *testing.T, retained map[string]string) *harness {
	t.Helper()
	h := &harness{t: t, writer: &fakeWriter{}}

	addr := freeAddr(t)
	h.broker = mqttserver.New(&mqttserver.Options{InlineClient: true})
	if err := h.broker.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("add hook: %v", err)
	}
	if err := h.broker.AddListener(listeners.NewTCP(listeners.Config{ID: "tcp", Address: addr})); err != nil {
		t.Fatalf("add listener: %v", err)
	}
	if err := h.broker.Serve(); err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { _ = h.broker.Close() })

	for topic, payload := range retained {
		h.publish(topic, payload, true)
	}

	h.reader = sdkmetric.NewManualReader()
	m, err := metrics.New(sdkmetric.NewMeterProvider(sdkmetric.WithReader(h.reader)).Meter("test"))
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}

	h.processor = NewProcessor(zigbee.NewRegistry("zigbee2mqtt"), h.writer, m, func() time.Time { return fixedTime })
	t.Cleanup(h.processor.Close)
	h.client = NewMQTTClient("tcp://"+addr, func(c paho.Client) {
		h.connects.Add(1)
		h.processor.Subscribe(c)
	},
		WithMQTTClientID(testClientID),
		WithMQTTMaxReconnectInterval(100*time.Millisecond),
	)
	if err := h.client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(h.client.Disconnect)

	h.eventually("first connect", func() bool { return h.connects.Load() >= 1 })
	// Connected is not subscribed: publishing before the SUBACK loses the
	// message, so wait until the broker holds the subscription.
	h.waitSubscribed()
	return h
}

func (h *harness) waitSubscribed() {
	h.t.Helper()
	h.eventually("subscribed", func() bool {
		c, ok := h.broker.Clients.Get(testClientID)
		return ok && !c.Closed() && len(c.State.Subscriptions.GetAll()) > 0
	})
}

func (h *harness) publish(topic, payload string, retain bool) {
	h.t.Helper()
	if err := h.broker.Publish(topic, []byte(payload), retain, 1); err != nil {
		h.t.Fatalf("publish %s: %v", topic, err)
	}
}

func (h *harness) eventually(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

// settle gives the client time to deliver anything still in flight, for
// assertions that something did NOT happen.
func (h *harness) settle() { time.Sleep(300 * time.Millisecond) }

// counter returns the value of a counter, optionally for one attribute value
// of "reason".
func (h *harness) counter(name, reason string) int64 {
	h.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		h.t.Fatalf("collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				h.t.Fatalf("%s is %T, want Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				if reason != "" {
					if v, ok := dp.Attributes.Value("reason"); !ok || v.AsString() != reason {
						continue
					}
				}
				total += dp.Value
			}
		}
	}
	return total
}

func TestProcessorStoresDevice(t *testing.T) {
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devicesJSON})

	h.eventually("registry", func() bool { r, _ := h.processor.registry.Route("zigbee2mqtt/office"); return r == zigbee.RouteDevice })
	h.publish("zigbee2mqtt/office", sensorJSON, false)
	h.eventually("write", func() bool { return len(h.writer.all()) == 1 })

	w := h.writer.all()[0]
	if w.device != (zigbee.Device{IEEE: "0x01", Name: "office"}) {
		t.Errorf("device = %+v, want {0x01 office}", w.device)
	}
	if !w.at.Equal(fixedTime) {
		t.Errorf("time = %v, want %v", w.at, fixedTime)
	}
	if len(w.readings) != 4 {
		t.Errorf("got %d readings, want 4 (battery, humidity, linkquality, temperature)", len(w.readings))
	}
	if got := h.counter("zigbee_rows_inserted", ""); got != 4 {
		t.Errorf("zigbee_rows_inserted = %d, want 4", got)
	}
}

func TestProcessorNestedName(t *testing.T) {
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devicesJSON})

	h.eventually("registry", func() bool {
		r, _ := h.processor.registry.Route("zigbee2mqtt/kitchen/temp")
		return r == zigbee.RouteDevice
	})
	h.publish("zigbee2mqtt/kitchen/temp", sensorJSON, false)
	h.eventually("write", func() bool { return len(h.writer.all()) == 1 })

	if d := h.writer.all()[0].device; d != (zigbee.Device{IEEE: "0x02", Name: "kitchen/temp"}) {
		t.Errorf("device = %+v, want {0x02 kitchen/temp}", d)
	}
}

func TestProcessorUnknownDeviceDropped(t *testing.T) {
	h := newHarness(t, nil) // no bridge/devices yet

	h.publish("zigbee2mqtt/office", sensorJSON, false)
	h.eventually("drop counted", func() bool { return h.counter("zigbee_messages_dropped", "unknown_device") == 1 })
	h.settle()

	if n := len(h.writer.all()); n != 0 {
		t.Errorf("got %d writes, want 0", n)
	}
}

func TestProcessorGroupIgnored(t *testing.T) {
	h := newHarness(t, map[string]string{
		"zigbee2mqtt/bridge/devices": devicesJSON,
		"zigbee2mqtt/bridge/groups":  groupsJSON,
	})

	h.eventually("registry", func() bool {
		r, _ := h.processor.registry.Route("zigbee2mqtt/downstairs")
		return r == zigbee.RouteGroup
	})
	h.publish("zigbee2mqtt/downstairs", `{"state":"ON"}`, false)
	h.settle()

	if n := len(h.writer.all()); n != 0 {
		t.Errorf("got %d writes, want 0", n)
	}
	if got := h.counter("zigbee_messages_dropped", ""); got != 0 {
		t.Errorf("zigbee_messages_dropped = %d, want 0", got)
	}
}

func TestProcessorDBErrorCounted(t *testing.T) {
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devicesJSON})
	h.writer.err = errors.New("database unavailable")

	h.eventually("registry", func() bool { r, _ := h.processor.registry.Route("zigbee2mqtt/office"); return r == zigbee.RouteDevice })
	h.publish("zigbee2mqtt/office", sensorJSON, false)
	h.eventually("db_error counted", func() bool { return h.counter("zigbee_messages_dropped", "db_error") == 1 })

	if got := h.counter("zigbee_rows_inserted", ""); got != 0 {
		t.Errorf("zigbee_rows_inserted = %d, want 0", got)
	}
}

func TestProcessorParseErrorCounted(t *testing.T) {
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devicesJSON})

	h.eventually("registry", func() bool { r, _ := h.processor.registry.Route("zigbee2mqtt/office"); return r == zigbee.RouteDevice })
	h.publish("zigbee2mqtt/office", `not json`, false)
	h.eventually("parse_error counted", func() bool { return h.counter("zigbee_messages_dropped", "parse_error") == 1 })

	if n := len(h.writer.all()); n != 0 {
		t.Errorf("got %d writes, want 0", n)
	}
}

func TestProcessorNoDuplicatesAfterReconnect(t *testing.T) {
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devicesJSON})

	cl, ok := h.broker.Clients.Get(testClientID)
	if !ok {
		t.Fatal("client not found on broker")
	}
	cl.Stop(fmt.Errorf("kicked by test"))
	h.eventually("reconnect", func() bool { return h.connects.Load() >= 2 })
	h.waitSubscribed()

	h.publish("zigbee2mqtt/office", sensorJSON, false)
	h.eventually("write", func() bool { return len(h.writer.all()) >= 1 })
	h.settle()

	if n := len(h.writer.all()); n != 1 {
		t.Errorf("got %d writes after reconnect, want exactly 1", n)
	}
}

// blockingWriter holds every write until release is closed.
type blockingWriter struct {
	fakeWriter
	release chan struct{}
}

func (b *blockingWriter) WriteReadings(ctx context.Context, t time.Time, d zigbee.Device, rs []zigbee.Reading) error {
	<-b.release
	return b.fakeWriter.WriteReadings(ctx, t, d, rs)
}

// A slow or hung database must not block Handle: Handle runs on paho's
// router goroutine, and blocking it stalls the network loop (PINGRESP
// included) until the broker connection times out.
func TestHandleDoesNotBlockOnSlowWriter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	m, err := metrics.New(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	reg := zigbee.NewRegistry("zigbee2mqtt")
	if err := reg.SetDevices([]byte(devicesJSON)); err != nil {
		t.Fatalf("SetDevices: %v", err)
	}
	w := &blockingWriter{release: make(chan struct{})}
	p := NewProcessor(reg, w, m, func() time.Time { return fixedTime })

	// One write in flight plus a full queue, then more that must be dropped.
	sent := writeQueueSize + 5
	done := make(chan struct{})
	go func() {
		for range sent {
			p.Handle("zigbee2mqtt/office", []byte(sensorJSON))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Handle blocked while the writer was stalled")
	}

	h := &harness{t: t, reader: reader}
	dropped := h.counter("zigbee_messages_dropped", metrics.ReasonQueueFull)
	if dropped < 1 {
		t.Errorf("queue_full drops = %d, want >= 1", dropped)
	}

	close(w.release)
	p.Close()

	if got := int64(len(w.all())) + dropped; got != int64(sent) {
		t.Errorf("writes (%d) + queue_full drops (%d) = %d, want %d", len(w.all()), dropped, got, sent)
	}
}

func TestProcessorStoresUnits(t *testing.T) {
	devices := `[{"ieee_address":"0x01","friendly_name":"office","type":"EndDevice","definition":{"exposes":[
		{"type":"numeric","property":"temperature","unit":"°C"},
		{"type":"numeric","property":"linkquality","unit":"lqi"}]}}]`
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devices})

	h.eventually("units upserted", func() bool { return len(h.writer.allUnits()) == 1 })

	want := []zigbee.MetricUnit{
		{IEEE: "0x01", Metric: "linkquality", Unit: "lqi"},
		{IEEE: "0x01", Metric: "temperature", Unit: "°C"},
	}
	if got := h.writer.allUnits()[0]; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("units = %+v, want %+v", got, want)
	}
}

func TestProcessorNoUnitsNoUpsert(t *testing.T) {
	h := newHarness(t, map[string]string{"zigbee2mqtt/bridge/devices": devicesJSON}) // no exposes

	h.eventually("registry", func() bool { r, _ := h.processor.registry.Route("zigbee2mqtt/office"); return r == zigbee.RouteDevice })
	h.settle()

	if n := len(h.writer.allUnits()); n != 0 {
		t.Errorf("got %d unit upserts, want 0", n)
	}
}
