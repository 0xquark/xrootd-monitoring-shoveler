package collector

import (
	"encoding/json"
	"testing"
	"time"
)

// TestConfiguredVOStampedOnRecord covers the configured collector VO: it becomes
// the VO of records on the main exchange (the raw feed) and of WLCG records
// alike, and an unset value leaves records exactly as the packet reported them.
func TestConfiguredVOStampedOnRecord(t *testing.T) {
	newCorrelator := func(vo string) *Correlator {
		return NewCorrelatorWithConfig(CorrelatorConfig{
			TTL:        5 * time.Second,
			MaxEntries: 0,
			VO:         vo,
		})
	}

	t.Run("stamped on a main-exchange record", func(t *testing.T) {
		correlator := newCorrelator("alice")
		defer correlator.Stop()

		// No VO and a non-WLCG path: the record the raw feed receives.
		record := &CollectorRecord{Filename: "/alice/data/file.root"}

		enriched, err := correlator.buildEnrichedRecord(record, "wlcg-exchange")
		if err != nil {
			t.Fatalf("buildEnrichedRecord() error = %v", err)
		}

		if enriched.Exchange != "" {
			t.Fatalf("Exchange = %q, expected the main exchange", enriched.Exchange)
		}

		var decoded struct {
			VO string `json:"vo"`
		}
		if err := json.Unmarshal(enriched.Payload, &decoded); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if decoded.VO != "alice" {
			t.Errorf("vo = %q, expected alice", decoded.VO)
		}
	})

	t.Run("stamped on a WLCG record", func(t *testing.T) {
		correlator := newCorrelator("alice")
		defer correlator.Stop()

		record := &CollectorRecord{Filename: "/store/data/file.root"}

		enriched, err := correlator.buildEnrichedRecord(record, "wlcg-exchange")
		if err != nil {
			t.Fatalf("buildEnrichedRecord() error = %v", err)
		}

		if enriched.Exchange != "wlcg-exchange" {
			t.Fatalf("Exchange = %q, expected wlcg-exchange", enriched.Exchange)
		}

		var decoded struct {
			VO       string                 `json:"vo"`
			Metadata map[string]interface{} `json:"metadata"`
		}
		if err := json.Unmarshal(enriched.Payload, &decoded); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if decoded.VO != "alice" {
			t.Errorf("vo = %q, expected alice", decoded.VO)
		}

		if _, ok := decoded.Metadata["vo"]; ok {
			t.Errorf("metadata should carry no vo field, got %v", decoded.Metadata["vo"])
		}
	})

	t.Run("replaces a VO from the packet stream", func(t *testing.T) {
		correlator := newCorrelator("alice")
		defer correlator.Stop()

		record := &CollectorRecord{VO: "cms", Filename: "/other/path"}

		if _, err := correlator.buildEnrichedRecord(record, "wlcg-exchange"); err != nil {
			t.Fatalf("buildEnrichedRecord() error = %v", err)
		}

		if record.VO != "alice" {
			t.Errorf("record VO = %q, expected alice", record.VO)
		}
	})

	t.Run("unset VO leaves the record alone", func(t *testing.T) {
		correlator := newCorrelator("")
		defer correlator.Stop()

		record := &CollectorRecord{VO: "cms", Filename: "/other/path"}

		enriched, err := correlator.buildEnrichedRecord(record, "wlcg-exchange")
		if err != nil {
			t.Fatalf("buildEnrichedRecord() error = %v", err)
		}

		if record.VO != "cms" {
			t.Errorf("record VO = %q, expected cms", record.VO)
		}

		var decoded struct {
			VO string `json:"vo"`
		}
		if err := json.Unmarshal(enriched.Payload, &decoded); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}

		if decoded.VO != "cms" {
			t.Errorf("vo = %q, expected cms", decoded.VO)
		}
	})
}

// TestConfiguredVODoesNotAffectRouting pins the ordering: routing and the drop
// filter match on the packet's own VO, never on the configured one, so setting
// it cannot move a record between exchanges or cause it to be dropped.
func TestConfiguredVODoesNotAffectRouting(t *testing.T) {
	correlator := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:        5 * time.Second,
		MaxEntries: 0,
		VO:         "cms", // would match WLCG routing if it were applied first
		WLCGVOs:    []string{"cms"},
		DropVOs:    []string{"cms"},
	})
	defer correlator.Stop()

	record := &CollectorRecord{VO: "alice", Filename: "/alice/data/file.root"}

	if drop, _ := correlator.shouldDrop(record); drop {
		t.Error("record should not be dropped: the drop filter sees the packet VO")
	}

	if correlator.matchesWLCG(record) {
		t.Error("record should not route to WLCG: routing sees the packet VO")
	}

	enriched, err := correlator.buildEnrichedRecord(record, "wlcg-exchange")
	if err != nil {
		t.Fatalf("buildEnrichedRecord() error = %v", err)
	}

	if enriched.Exchange != "" {
		t.Errorf("Exchange = %q, expected the main exchange", enriched.Exchange)
	}
}

// TestEmptyRoutingListsAlwaysUseCollectorRecord pins the "disable WLCG routing"
// switch: with both lists empty, even a record that would match every default
// rule (VO cms, /store path) skips the WLCG branch entirely and is published as
// a plain collector record on the main exchange.
func TestEmptyRoutingListsAlwaysUseCollectorRecord(t *testing.T) {
	correlator := NewCorrelatorWithConfig(CorrelatorConfig{
		TTL:              5 * time.Second,
		MaxEntries:       0,
		WLCGVOs:          []string{},
		WLCGPathPrefixes: []string{},
	})
	defer correlator.Stop()

	record := &CollectorRecord{VO: "cms", Filename: "/store/data/file.root"}

	if correlator.matchesWLCG(record) {
		t.Fatal("empty routing lists should match nothing")
	}

	enriched, err := correlator.buildEnrichedRecord(record, "wlcg-exchange")
	if err != nil {
		t.Fatalf("buildEnrichedRecord() error = %v", err)
	}

	if enriched.Exchange != "" {
		t.Errorf("Exchange = %q, expected the main exchange", enriched.Exchange)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(enriched.Payload, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	// Collector-record shape, not WLCG: "filename"/"serverID", never "file_lfn".
	if _, ok := decoded["filename"]; !ok {
		t.Error("payload should be a collector record (filename)")
	}

	for _, wlcgOnly := range []string{"file_lfn", "unique_id", "site_name", "metadata"} {
		if _, ok := decoded[wlcgOnly]; ok {
			t.Errorf("payload should not carry the WLCG field %q", wlcgOnly)
		}
	}
}
