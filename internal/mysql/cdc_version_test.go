package mysql

import (
	"sync/atomic"
	"testing"
)

func TestVersionCounterUniqueness(t *testing.T) {
	// Simulate two events with the same binlog timestamp (same transaction)
	var counter uint64
	timestamp := uint32(1710300000)

	version1 := (uint64(timestamp) << 32) | (atomic.AddUint64(&counter, 1) & 0xFFFFFFFF)
	version2 := (uint64(timestamp) << 32) | (atomic.AddUint64(&counter, 1) & 0xFFFFFFFF)

	if version1 == version2 {
		t.Errorf("versions should be unique, both are %d", version1)
	}
	if version1 >= version2 {
		t.Errorf("version1 (%d) should be less than version2 (%d)", version1, version2)
	}
}

func TestVersionCounterCrossSecondOrdering(t *testing.T) {
	// Events from second N+1 should always have higher versions than second N
	var counter uint64
	ts1 := uint32(1710300000)
	ts2 := uint32(1710300001)

	v1 := (uint64(ts1) << 32) | (atomic.AddUint64(&counter, 1) & 0xFFFFFFFF)
	v2 := (uint64(ts2) << 32) | (atomic.AddUint64(&counter, 1) & 0xFFFFFFFF)

	if v1 >= v2 {
		t.Errorf("later timestamp should produce higher version: v1=%d, v2=%d", v1, v2)
	}
}

func TestVersionCounterCrossSecondOrderingWithCounterReset(t *testing.T) {
	// Even if counter resets (e.g., after restart), later timestamps dominate
	ts1 := uint32(1710300000)
	ts2 := uint32(1710300001)

	var counter1 uint64 = 999 // high counter before "restart"
	var counter2 uint64 = 0   // reset counter after "restart"

	v1 := (uint64(ts1) << 32) | (atomic.AddUint64(&counter1, 1) & 0xFFFFFFFF)
	v2 := (uint64(ts2) << 32) | (atomic.AddUint64(&counter2, 1) & 0xFFFFFFFF)

	if v1 >= v2 {
		t.Errorf("later timestamp should produce higher version even with counter reset: v1=%d, v2=%d", v1, v2)
	}
}
