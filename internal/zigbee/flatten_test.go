package zigbee

import (
	"reflect"
	"strconv"
	"testing"
)

func num(v float64) *float64 { return &v }
func str(s string) *string   { return &s }

func TestFlatten(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    []Reading
	}{
		{
			name: "third reality temp/humidity sensor",
			payload: `{"battery":100,"celsius_degree_calibration":0,"fahrenheit_degree_calibration":0,
				"humidity":51.72,"humidity_calibration":0,"linkquality":112,"temperature":25.81,
				"update":{"installed_version":15,"latest_version":15,"state":"idle"}}`,
			want: []Reading{
				{Metric: "battery", Value: num(100)},
				{Metric: "humidity", Value: num(51.72)},
				{Metric: "linkquality", Value: num(112)},
				{Metric: "temperature", Value: num(25.81)},
			},
		},
		{
			name:    "booleans and ON become 1/0",
			payload: `{"state":"ON","contact":false,"occupancy":true}`,
			want: []Reading{
				{Metric: "contact", Value: num(0)},
				{Metric: "occupancy", Value: num(1)},
				{Metric: "state", Value: num(1)},
			},
		},
		{name: "OFF", payload: `{"state":"OFF"}`, want: []Reading{{Metric: "state", Value: num(0)}}},
		{name: "OPEN", payload: `{"state":"OPEN"}`, want: []Reading{{Metric: "state", Value: num(1)}}},
		{name: "CLOSED", payload: `{"state":"CLOSED"}`, want: []Reading{{Metric: "state", Value: num(0)}}},
		{name: "LOCK", payload: `{"state":"LOCK"}`, want: []Reading{{Metric: "state", Value: num(1)}}},
		{name: "UNLOCK", payload: `{"state":"UNLOCK"}`, want: []Reading{{Metric: "state", Value: num(0)}}},
		{
			name:    "other strings are text",
			payload: `{"action":"single"}`,
			want:    []Reading{{Metric: "action", TextValue: str("single")}},
		},
		{
			name:    "nested objects and nulls are skipped",
			payload: `{"color":{"x":0.3},"power_on_behavior":null}`,
			want:    []Reading{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Flatten([]byte(tt.payload))
			if err != nil {
				t.Fatalf("Flatten: unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Flatten mismatch\n got: %s\nwant: %s", describe(got), describe(tt.want))
			}
		})
	}
}

func TestFlattenRejectsNonObject(t *testing.T) {
	for _, payload := range []string{`"online"`, `[]`, ``, `{bad`} {
		if _, err := Flatten([]byte(payload)); err == nil {
			t.Errorf("Flatten(%q): expected error, got nil", payload)
		}
	}
}

func describe(rs []Reading) string {
	s := "["
	for _, r := range rs {
		s += r.Metric + "="
		switch {
		case r.Value != nil:
			s += strconv.FormatFloat(*r.Value, 'g', -1, 64)
		case r.TextValue != nil:
			s += `"` + *r.TextValue + `"`
		default:
			s += "<nil>"
		}
		s += " "
	}
	return s + "]"
}
