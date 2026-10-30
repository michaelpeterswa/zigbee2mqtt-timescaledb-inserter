package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"alpineworks.io/ootel"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"

	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/config"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/logging"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/metrics"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/mqtt"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/timescale"
	"github.com/michaelpeterswa/zigbee2mqtt-timescaledb-inserter/internal/zigbee"
)

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	slogLevel, err := logging.LogLevelToSlogLevel(logLevel)
	if err != nil {
		log.Fatalf("could not convert log level: %s", err)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slogLevel,
	})))
	c, err := config.NewConfig()
	if err != nil {
		slog.Error("could not create config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx := context.Background()

	exporterType := ootel.ExporterTypePrometheus
	if c.Local {
		exporterType = ootel.ExporterTypeOTLPGRPC
	}

	ootelClient := ootel.NewOotelClient(
		ootel.WithMetricConfig(
			ootel.NewMetricConfig(
				c.MetricsEnabled,
				exporterType,
				c.MetricsPort,
			),
		),
		ootel.WithTraceConfig(
			ootel.NewTraceConfig(
				c.TracingEnabled,
				c.TracingSampleRate,
				c.TracingService,
				c.TracingVersion,
			),
		),
	)

	shutdown, err := ootelClient.Init(ctx)
	if err != nil {
		slog.Error("could not create ootel client", slog.String("error", err.Error()))
		os.Exit(1)
	}

	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(5 * time.Second))
	if err != nil {
		slog.Error("could not create runtime metrics", slog.String("error", err.Error()))
		os.Exit(1)
	}

	err = host.Start()
	if err != nil {
		slog.Error("could not create host metrics", slog.String("error", err.Error()))
		os.Exit(1)
	}

	defer func() {
		_ = shutdown(ctx)
	}()

	m, err := metrics.New(otel.Meter("zigbee2mqtt-timescaledb-inserter"))
	if err != nil {
		slog.Error("could not create metrics", slog.String("error", err.Error()))
		os.Exit(1)
	}

	timescaleClient, err := timescale.NewTimescaleClient(ctx, c.TimescaleConnString)
	if err != nil {
		slog.Error("could not create timescale client", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer timescaleClient.Close()

	processor := mqtt.NewProcessor(zigbee.NewRegistry(c.MQTTBaseTopic), timescaleClient, m, time.Now)

	// processor.Subscribe is the only subscriber: it runs on every
	// (re)connect, so subscriptions survive reconnects without stacking.
	mqttClient := mqtt.NewMQTTClient(c.MQTTAddress, processor.Subscribe,
		mqtt.WithMQTTClientID(c.MQTTClientID),
		mqtt.WithMQTTKeepAlive(c.MQTTKeepAlive),
		mqtt.WithMQTTPingTimeout(c.MQTTPingTimeout),
		mqtt.WithMQTTConnectTimeout(c.MQTTConnectTimeout),
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	err = mqttClient.Connect()
	if err != nil {
		slog.Error("could not connect to mqtt broker", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slog.Info("zigbee2mqtt-timescaledb-inserter started", slog.String("base_topic", c.MQTTBaseTopic))

	<-sigChan
	slog.Info("shutdown signal received, disconnecting...")
	mqttClient.Disconnect()
	processor.Close()
}
