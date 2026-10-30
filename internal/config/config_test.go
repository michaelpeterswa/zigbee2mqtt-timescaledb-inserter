package config

import (
	"testing"
	"time"
)

func TestNewConfigDefaults(t *testing.T) {
	t.Setenv("TIMESCALE_CONN_STRING", "postgres://test")

	c, err := NewConfig()
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"LogLevel", c.LogLevel, "info"},
		{"MQTTAddress", c.MQTTAddress, "tcp://localhost:1883"},
		{"MQTTClientID", c.MQTTClientID, "zigbee2mqtt-timescaledb-inserter"},
		{"MQTTBaseTopic", c.MQTTBaseTopic, "zigbee2mqtt"},
		{"MQTTKeepAlive", c.MQTTKeepAlive, 30 * time.Second},
		{"MQTTPingTimeout", c.MQTTPingTimeout, 10 * time.Second},
		{"MQTTConnectTimeout", c.MQTTConnectTimeout, 30 * time.Second},
		{"MetricsEnabled", c.MetricsEnabled, true},
		{"MetricsPort", c.MetricsPort, 8081},
		{"Local", c.Local, false},
		{"TracingService", c.TracingService, "zigbee2mqtt-timescaledb-inserter"},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
}

func TestNewConfigFromEnv(t *testing.T) {
	t.Setenv("TIMESCALE_CONN_STRING", "postgres://test")
	t.Setenv("MQTT_ADDRESS", "tcp://broker:1883")
	t.Setenv("MQTT_BASE_TOPIC", "z2m")

	c, err := NewConfig()
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}
	if c.MQTTAddress != "tcp://broker:1883" || c.MQTTBaseTopic != "z2m" {
		t.Errorf("got (%s, %s), want (tcp://broker:1883, z2m)", c.MQTTAddress, c.MQTTBaseTopic)
	}
}

func TestNewConfigRequiresConnString(t *testing.T) {
	t.Setenv("TIMESCALE_CONN_STRING", "")

	if _, err := NewConfig(); err == nil {
		t.Fatal("NewConfig without TIMESCALE_CONN_STRING: expected error")
	}
}
