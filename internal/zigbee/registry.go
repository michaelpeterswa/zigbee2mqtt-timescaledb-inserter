package zigbee

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Device is a zigbee2mqtt device: its stable IEEE address and its current
// friendly name.
type Device struct {
	IEEE string
	Name string
}

// Route says what a message on a topic is.
type Route int

const (
	RouteIgnore Route = iota
	RouteBridgeDevices
	RouteBridgeGroups
	RouteDevice
	RouteGroup
	RouteUnknown
)

// ignoredSuffixes are per-device subtopics that never carry device state.
var ignoredSuffixes = []string{"/availability", "/set", "/get"}

// Registry maps zigbee2mqtt topics to devices using the retained
// bridge/devices and bridge/groups messages. Friendly names may contain
// "/", so topics are matched exactly against known names rather than by
// topic level.
type Registry struct {
	baseTopic string

	mu      sync.RWMutex
	devices map[string]Device // by friendly name
	groups  map[string]bool   // by friendly name
}

func NewRegistry(baseTopic string) *Registry {
	return &Registry{
		baseTopic: baseTopic,
		devices:   map[string]Device{},
		groups:    map[string]bool{},
	}
}

// SetDevices replaces the known devices from a bridge/devices payload. The
// coordinator is skipped. On error the known devices are left unchanged.
func (r *Registry) SetDevices(payload []byte) error {
	var entries []struct {
		IEEEAddress  string `json:"ieee_address"`
		FriendlyName string `json:"friendly_name"`
		Type         string `json:"type"`
	}
	if err := json.Unmarshal(payload, &entries); err != nil {
		return fmt.Errorf("decode bridge/devices: %w", err)
	}

	devices := make(map[string]Device, len(entries))
	for _, e := range entries {
		if e.Type == "Coordinator" || e.IEEEAddress == "" || e.FriendlyName == "" {
			continue
		}
		devices[e.FriendlyName] = Device{IEEE: e.IEEEAddress, Name: e.FriendlyName}
	}

	r.mu.Lock()
	r.devices = devices
	r.mu.Unlock()

	return nil
}

// SetGroups replaces the known groups from a bridge/groups payload. On error
// the known groups are left unchanged.
func (r *Registry) SetGroups(payload []byte) error {
	var entries []struct {
		FriendlyName string `json:"friendly_name"`
	}
	if err := json.Unmarshal(payload, &entries); err != nil {
		return fmt.Errorf("decode bridge/groups: %w", err)
	}

	groups := make(map[string]bool, len(entries))
	for _, e := range entries {
		if e.FriendlyName != "" {
			groups[e.FriendlyName] = true
		}
	}

	r.mu.Lock()
	r.groups = groups
	r.mu.Unlock()

	return nil
}

// Route classifies a topic. The Device is only set for RouteDevice.
func (r *Registry) Route(topic string) (Route, Device) {
	name, ok := strings.CutPrefix(topic, r.baseTopic+"/")
	if !ok || name == "" {
		return RouteIgnore, Device{}
	}

	switch name {
	case "bridge/devices":
		return RouteBridgeDevices, Device{}
	case "bridge/groups":
		return RouteBridgeGroups, Device{}
	}
	if name == "bridge" || strings.HasPrefix(name, "bridge/") {
		return RouteIgnore, Device{}
	}

	// Checked before the name lookup so a device named e.g. "x/set" cannot
	// make command topics look like state.
	for _, suffix := range ignoredSuffixes {
		if strings.HasSuffix(name, suffix) || strings.Contains(name, suffix+"/") {
			return RouteIgnore, Device{}
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if d, ok := r.devices[name]; ok {
		return RouteDevice, d
	}
	if r.groups[name] {
		return RouteGroup, Device{}
	}
	return RouteUnknown, Device{}
}

// BaseTopic is the zigbee2mqtt base topic this registry routes under.
func (r *Registry) BaseTopic() string { return r.baseTopic }
