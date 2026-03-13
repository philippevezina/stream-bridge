package common

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventVersionSerialization(t *testing.T) {
	event := Event{
		ID:        "test-id",
		Type:      EventTypeUpdate,
		Database:  "db",
		Table:     "t",
		Timestamp: time.Unix(1000, 0),
		Version:   (1000 << 32) | 42,
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.Version != event.Version {
		t.Errorf("Version mismatch: got %d, want %d", decoded.Version, event.Version)
	}
}
