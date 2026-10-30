# zigbee2mqtt-timescaledb-inserter

Stores every reading from every [Zigbee2MQTT](https://www.zigbee2mqtt.io/)
device in a TimescaleDB hypertable, one row per metric, so new kinds of
devices need no schema change.

```
zigbee2mqtt ──MQTT──▶ inserter ──COPY──▶ sensors.zigbee
```

## What gets stored

Each device state message (`<base_topic>/<friendly_name>`) becomes one row
per field:

| column        | example              |
|---------------|----------------------|
| `time`        | when the message arrived (payloads carry no timestamp) |
| `device_ieee` | `0x00124b0012345678` — the stable key |
| `device_name` | `office` — the friendly name at that time |
| `metric`      | `temperature`, `linkquality`, `state`, … |
| `value`       | numbers; booleans and two-state strings as 1/0 |
| `text_value`  | any other string, e.g. a button's `action` |

Field mapping:

- **Numbers** are stored as-is in `value`.
- **Booleans** become `1`/`0`.
- **Two-state strings** become `1`/`0` (case-sensitive): `ON`/`OFF`,
  `OPEN`/`CLOSED`, `LOCK`/`UNLOCK`.
- **Other strings** go to `text_value`.
- **Skipped:** nested objects and arrays (e.g. `update`, `color`), `null`,
  the `update` key, and any key ending in `_calibration`. These are device
  settings, not readings.

Group topics, `bridge/*`, and `/availability`, `/set` and `/get` subtopics
are ignored. Friendly names may contain `/`.

Device names come from the retained `bridge/devices` message, which the
inserter re-reads whenever zigbee2mqtt republishes it (pair, rename, remove).
A message from a device not yet in that list is dropped and counted, never
stored under a guessed key.

**Retained state:** zigbee2mqtt does not retain device state by default. If
you turn `retain` on for a device, its last state is inserted once more each
time the inserter restarts.

## Schema

The table and the inserter's database role are created by migrations, not by
this service. The role only needs `INSERT` on the table:

```sql
CREATE TABLE sensors.zigbee (
    time        TIMESTAMPTZ NOT NULL,
    device_ieee TEXT NOT NULL,
    device_name TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION,
    text_value  TEXT,
    CHECK ((value IS NULL) <> (text_value IS NULL))
);
SELECT create_hypertable('sensors.zigbee', 'time');
CREATE INDEX ON sensors.zigbee (device_ieee, metric, time DESC);
```

## Configuration

| variable                | default                             |
|-------------------------|-------------------------------------|
| `TIMESCALE_CONN_STRING` | **required**                        |
| `MQTT_ADDRESS`          | `tcp://localhost:1883`              |
| `MQTT_BASE_TOPIC`       | `zigbee2mqtt`                       |
| `MQTT_CLIENT_ID`        | `zigbee2mqtt-timescaledb-inserter`  |
| `MQTT_KEEPALIVE`        | `30s`                               |
| `MQTT_PING_TIMEOUT`     | `10s`                               |
| `MQTT_CONNECT_TIMEOUT`  | `30s`                               |
| `LOG_LEVEL`             | `info`                              |
| `METRICS_ENABLED`       | `true`                              |
| `METRICS_PORT`          | `8081`                              |
| `LOCAL`                 | `false` (`true` exports OTLP instead of Prometheus) |
| `TRACING_ENABLED`, `TRACING_SAMPLERATE`, `TRACING_SERVICE`, `TRACING_VERSION` | tracing off |

The MQTT session is always clean and subscriptions are made only on connect,
so a reconnect never produces duplicate rows.

## Metrics

Prometheus metrics on `:8081/metrics`:

- `zigbee_messages_received_total`: device messages received
- `zigbee_rows_inserted_total`: rows written
- `zigbee_messages_dropped_total{reason}`: `unknown_device`, `parse_error`
  or `db_error`. If the database is down, messages are dropped rather than
  queued.
- `zigbee_insert_duration_seconds`: time per message insert

## Querying

Temperature per device, using each device's current name:

```sql
SELECT time_bucket('5 minutes', z.time) AS time,
       n.device_name AS device,
       avg(z.value) AS temperature
FROM sensors.zigbee z
JOIN LATERAL (
    SELECT device_name FROM sensors.zigbee
    WHERE device_ieee = z.device_ieee
    ORDER BY time DESC LIMIT 1
) n ON true
WHERE z.metric = 'temperature' AND z.time > now() - interval '24 hours'
GROUP BY 1, 2
ORDER BY 1;
```

## Development

```sh
# TimescaleDB with the schema applied from a timescale-migrations checkout
make testdb MIGRATIONS_DIR=path/to/timescale-migrations/migrations
TIMESCALE_TEST_CONN_STRING=$(make -s testdb-url) go test -race ./...
make testdb-stop
```

Without `TIMESCALE_TEST_CONN_STRING`, the database tests are skipped. The
MQTT tests run against an embedded broker.
