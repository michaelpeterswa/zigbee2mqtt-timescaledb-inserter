package zigbee

import (
	"encoding/json"
	"fmt"
	"sort"
)

// MetricUnit is the unit zigbee2mqtt declares for one metric of one device.
type MetricUnit struct {
	IEEE   string
	Metric string
	Unit   string
}

type expose struct {
	Property string   `json:"property"`
	Unit     string   `json:"unit"`
	Features []expose `json:"features"`
}

// ParseUnits extracts metric units from a bridge/devices payload, using each
// device definition's exposes (including nested features). Metrics without a
// unit, metrics Flatten skips, and the coordinator are left out. The result
// is sorted by IEEE address, then metric.
func ParseUnits(payload []byte) ([]MetricUnit, error) {
	var entries []struct {
		IEEEAddress string `json:"ieee_address"`
		Type        string `json:"type"`
		Definition  *struct {
			Exposes []expose `json:"exposes"`
		} `json:"definition"`
	}
	if err := json.Unmarshal(payload, &entries); err != nil {
		return nil, fmt.Errorf("decode bridge/devices: %w", err)
	}

	units := []MetricUnit{}
	for _, e := range entries {
		if e.Type == "Coordinator" || e.IEEEAddress == "" || e.Definition == nil {
			continue
		}
		seen := map[string]bool{}
		var walk func([]expose)
		walk = func(exposes []expose) {
			for _, x := range exposes {
				if x.Property != "" && x.Unit != "" && !skip(x.Property) && !seen[x.Property] {
					seen[x.Property] = true
					units = append(units, MetricUnit{IEEE: e.IEEEAddress, Metric: x.Property, Unit: x.Unit})
				}
				walk(x.Features)
			}
		}
		walk(e.Definition.Exposes)
	}

	sort.Slice(units, func(i, j int) bool {
		if units[i].IEEE != units[j].IEEE {
			return units[i].IEEE < units[j].IEEE
		}
		return units[i].Metric < units[j].Metric
	})

	return units, nil
}
