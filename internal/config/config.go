package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config is read from the environment. There are no knobs for MQTT QoS or
// clean session: subscriptions are made in the connect handler, which only
// stays duplicate-free with a clean session.
type Config struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	MQTTAddress        string        `env:"MQTT_ADDRESS" envDefault:"tcp://localhost:1883"`
	MQTTClientID       string        `env:"MQTT_CLIENT_ID" envDefault:"zigbee2mqtt-timescaledb-inserter"`
	MQTTBaseTopic      string        `env:"MQTT_BASE_TOPIC" envDefault:"zigbee2mqtt"`
	MQTTKeepAlive      time.Duration `env:"MQTT_KEEPALIVE" envDefault:"30s"`
	MQTTPingTimeout    time.Duration `env:"MQTT_PING_TIMEOUT" envDefault:"10s"`
	MQTTConnectTimeout time.Duration `env:"MQTT_CONNECT_TIMEOUT" envDefault:"30s"`

	TimescaleConnString string `env:"TIMESCALE_CONN_STRING,required,notEmpty"`

	MetricsEnabled bool `env:"METRICS_ENABLED" envDefault:"true"`
	MetricsPort    int  `env:"METRICS_PORT" envDefault:"8081"`

	Local bool `env:"LOCAL" envDefault:"false"`

	TracingEnabled    bool    `env:"TRACING_ENABLED" envDefault:"false"`
	TracingSampleRate float64 `env:"TRACING_SAMPLERATE" envDefault:"0.01"`
	TracingService    string  `env:"TRACING_SERVICE" envDefault:"zigbee2mqtt-timescaledb-inserter"`
	TracingVersion    string  `env:"TRACING_VERSION"`
}

func NewConfig() (*Config, error) {
	var cfg Config

	err := env.Parse(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &cfg, nil
}
