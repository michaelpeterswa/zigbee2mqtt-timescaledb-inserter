package zigbee

import "testing"

const testDevices = `[
	{"ieee_address":"0x00","friendly_name":"Coordinator","type":"Coordinator"},
	{"ieee_address":"0x01","friendly_name":"office","type":"EndDevice"},
	{"ieee_address":"0x02","friendly_name":"kitchen/temp","type":"EndDevice"}
]`

const testGroups = `[{"id":1,"friendly_name":"downstairs","members":[]}]`

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry("zigbee2mqtt")
	if err := r.SetDevices([]byte(testDevices)); err != nil {
		t.Fatalf("SetDevices: %v", err)
	}
	if err := r.SetGroups([]byte(testGroups)); err != nil {
		t.Fatalf("SetGroups: %v", err)
	}
	return r
}

func TestRoute(t *testing.T) {
	r := newTestRegistry(t)

	tests := []struct {
		topic      string
		wantRoute  Route
		wantDevice Device
	}{
		{"zigbee2mqtt/office", RouteDevice, Device{IEEE: "0x01", Name: "office"}},
		{"zigbee2mqtt/kitchen/temp", RouteDevice, Device{IEEE: "0x02", Name: "kitchen/temp"}},
		{"zigbee2mqtt/office/availability", RouteIgnore, Device{}},
		{"zigbee2mqtt/office/set", RouteIgnore, Device{}},
		{"zigbee2mqtt/office/set/state", RouteIgnore, Device{}},
		{"zigbee2mqtt/office/get", RouteIgnore, Device{}},
		{"zigbee2mqtt/bridge/state", RouteIgnore, Device{}},
		{"zigbee2mqtt/bridge/logging", RouteIgnore, Device{}},
		{"othertopic/x", RouteIgnore, Device{}},
		{"zigbee2mqtt/bridge/devices", RouteBridgeDevices, Device{}},
		{"zigbee2mqtt/bridge/groups", RouteBridgeGroups, Device{}},
		{"zigbee2mqtt/downstairs", RouteGroup, Device{}},
		{"zigbee2mqtt/0x03", RouteUnknown, Device{}},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			route, device := r.Route(tt.topic)
			if route != tt.wantRoute || device != tt.wantDevice {
				t.Errorf("Route(%q) = (%v, %+v), want (%v, %+v)", tt.topic, route, device, tt.wantRoute, tt.wantDevice)
			}
		})
	}
}

func TestRegistryRename(t *testing.T) {
	r := newTestRegistry(t)

	renamed := `[{"ieee_address":"0x01","friendly_name":"study","type":"EndDevice"}]`
	if err := r.SetDevices([]byte(renamed)); err != nil {
		t.Fatalf("SetDevices: %v", err)
	}

	if route, device := r.Route("zigbee2mqtt/study"); route != RouteDevice || device != (Device{IEEE: "0x01", Name: "study"}) {
		t.Errorf("Route(study) = (%v, %+v), want (RouteDevice, {0x01 study})", route, device)
	}
	if route, _ := r.Route("zigbee2mqtt/office"); route != RouteUnknown {
		t.Errorf("Route(office) after rename = %v, want RouteUnknown", route)
	}
}

func TestSetDevicesSkipsCoordinator(t *testing.T) {
	r := newTestRegistry(t)

	if route, _ := r.Route("zigbee2mqtt/Coordinator"); route != RouteUnknown {
		t.Errorf("Route(Coordinator) = %v, want RouteUnknown", route)
	}
}

func TestSetDevicesRejectsBadJSON(t *testing.T) {
	r := newTestRegistry(t)

	if err := r.SetDevices([]byte(`{bad`)); err == nil {
		t.Fatal("SetDevices(bad JSON): expected error")
	}
	// A bad publish must not wipe the known devices.
	if route, _ := r.Route("zigbee2mqtt/office"); route != RouteDevice {
		t.Errorf("Route(office) after bad SetDevices = %v, want RouteDevice", route)
	}
}
