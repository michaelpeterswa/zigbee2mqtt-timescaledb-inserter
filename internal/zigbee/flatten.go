package zigbee

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Reading is one metric from one device message. Exactly one of Value and
// TextValue is set.
type Reading struct {
	Metric    string
	Value     *float64
	TextValue *string
}

// SkipSuffixes are key suffixes for device settings rather than readings.
var SkipSuffixes = []string{"_calibration"}

// SkipKeys are keys that are never readings.
var SkipKeys = map[string]bool{"update": true}

// BinaryStates maps the state strings zigbee2mqtt sends for two-state
// devices to 1/0, so they can be graphed. Matching is case-sensitive.
var BinaryStates = map[string]float64{
	"ON": 1, "OFF": 0,
	"OPEN": 1, "CLOSED": 0,
	"LOCK": 1, "UNLOCK": 0,
}

// Flatten turns one zigbee2mqtt device payload into readings, sorted by
// metric. Nested objects, arrays, nulls and setting keys are skipped. It
// returns an error if the payload is not exactly one JSON object.
func Flatten(payload []byte) ([]Reading, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()

	var fields map[string]any
	if err := dec.Decode(&fields); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if fields == nil {
		return nil, fmt.Errorf("payload is not a JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("payload has data after the JSON object")
	}

	readings := []Reading{}
	for key, raw := range fields {
		if skip(key) {
			continue
		}

		switch v := raw.(type) {
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				continue
			}
			readings = append(readings, Reading{Metric: key, Value: &f})
		case bool:
			f := 0.0
			if v {
				f = 1
			}
			readings = append(readings, Reading{Metric: key, Value: &f})
		case string:
			if f, ok := BinaryStates[v]; ok {
				readings = append(readings, Reading{Metric: key, Value: &f})
				continue
			}
			readings = append(readings, Reading{Metric: key, TextValue: &v})
		}
	}

	sort.Slice(readings, func(i, j int) bool { return readings[i].Metric < readings[j].Metric })

	return readings, nil
}

func skip(key string) bool {
	if SkipKeys[key] {
		return true
	}
	for _, suffix := range SkipSuffixes {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}
