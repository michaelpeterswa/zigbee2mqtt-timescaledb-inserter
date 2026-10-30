package zigbee

import (
	"reflect"
	"testing"
)

// Shapes taken from zigbee2mqtt's bridge/devices: plain exposes, and
// composite ones (lights, climate) whose properties sit under "features".
const unitDevices = `[
	{"ieee_address":"0x00","friendly_name":"Coordinator","type":"Coordinator"},
	{"ieee_address":"0x01","friendly_name":"office","type":"EndDevice","definition":{"exposes":[
		{"type":"numeric","property":"battery","unit":"%"},
		{"type":"numeric","property":"temperature","unit":"°C"},
		{"type":"numeric","property":"temperature_calibration","unit":"°C"},
		{"type":"enum","property":"action"},
		{"type":"numeric","property":"linkquality","unit":"lqi"}
	]}},
	{"ieee_address":"0x02","friendly_name":"plug","type":"Router","definition":{"exposes":[
		{"type":"switch","features":[{"type":"binary","property":"state"}]},
		{"type":"numeric","property":"power","unit":"W"},
		{"type":"climate","features":[{"type":"numeric","property":"local_temperature","unit":"°C"}]}
	]}},
	{"ieee_address":"0x03","friendly_name":"unsupported","type":"EndDevice","definition":null}
]`

func TestParseUnits(t *testing.T) {
	got, err := ParseUnits([]byte(unitDevices))
	if err != nil {
		t.Fatalf("ParseUnits: %v", err)
	}

	want := []MetricUnit{
		{IEEE: "0x01", Metric: "battery", Unit: "%"},
		{IEEE: "0x01", Metric: "linkquality", Unit: "lqi"},
		{IEEE: "0x01", Metric: "temperature", Unit: "°C"},
		{IEEE: "0x02", Metric: "local_temperature", Unit: "°C"},
		{IEEE: "0x02", Metric: "power", Unit: "W"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseUnits\n got: %+v\nwant: %+v", got, want)
	}
}

func TestParseUnitsRejectsBadJSON(t *testing.T) {
	if _, err := ParseUnits([]byte(`{bad`)); err == nil {
		t.Fatal("ParseUnits(bad JSON): expected error")
	}
}
