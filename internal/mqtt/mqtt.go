package mqtt

import (
	"fmt"
	"log/slog"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// MQTTClient is a thin wrapper around paho. Unlike birdnet-mqtt-timescaledb
// it keeps no subscription list of its own: the onConnect callback is the
// only place subscriptions are made, so every (re)connect subscribes exactly
// once and a reconnect can never stack a second handler.
type MQTTClient struct {
	client paho.Client
}

type MQTTClientOption func(*paho.ClientOptions)

func WithMQTTClientID(clientID string) MQTTClientOption {
	return func(opts *paho.ClientOptions) {
		opts.SetClientID(clientID)
	}
}

func WithMQTTKeepAlive(keepAlive time.Duration) MQTTClientOption {
	return func(opts *paho.ClientOptions) {
		opts.SetKeepAlive(keepAlive)
	}
}

func WithMQTTPingTimeout(pingTimeout time.Duration) MQTTClientOption {
	return func(opts *paho.ClientOptions) {
		opts.SetPingTimeout(pingTimeout)
	}
}

func WithMQTTConnectTimeout(connectTimeout time.Duration) MQTTClientOption {
	return func(opts *paho.ClientOptions) {
		opts.SetConnectTimeout(connectTimeout)
	}
}

func WithMQTTMaxReconnectInterval(interval time.Duration) MQTTClientOption {
	return func(opts *paho.ClientOptions) {
		opts.SetMaxReconnectInterval(interval)
	}
}

// NewMQTTClient creates a client with a clean session and auto-reconnect.
// onConnect runs after every successful connect and must make all
// subscriptions.
func NewMQTTClient(broker string, onConnect func(paho.Client), options ...MQTTClientOption) *MQTTClient {
	opts := paho.NewClientOptions().
		AddBroker(broker).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			slog.Error("MQTT connection lost", slog.String("error", err.Error()))
		}).
		SetOnConnectHandler(func(c paho.Client) {
			slog.Info("MQTT connection established")
			onConnect(c)
		}).
		SetReconnectingHandler(func(_ paho.Client, _ *paho.ClientOptions) {
			slog.Warn("MQTT attempting to reconnect")
		})

	for _, option := range options {
		option(opts)
	}

	return &MQTTClient{client: paho.NewClient(opts)}
}

func (m *MQTTClient) Connect() error {
	token := m.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("connect to MQTT broker: %w", err)
	}
	return nil
}

func (m *MQTTClient) Disconnect() {
	m.client.Disconnect(250)
}
