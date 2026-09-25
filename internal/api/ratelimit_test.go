package api

import (
	"testing"
	"time"
)

func TestRateLimiterAllowsUpToBurstThenBlocks(t *testing.T) {
	rl := newRateLimiter(2, time.Hour) // long interval: refills won't fire mid-test

	if !rl.Allow("alice") {
		t.Error("first request should be allowed")
	}
	if !rl.Allow("alice") {
		t.Error("second request should be allowed")
	}
	if rl.Allow("alice") {
		t.Error("third request should be blocked, burst of 2 exhausted")
	}
}

func TestRateLimiterTracksOwnersSeparately(t *testing.T) {
	rl := newRateLimiter(1, time.Hour)

	if !rl.Allow("alice") {
		t.Error("alice's first request should be allowed")
	}
	if !rl.Allow("bob") {
		t.Error("bob should have his own bucket, unaffected by alice")
	}
	if rl.Allow("alice") {
		t.Error("alice's second request should be blocked")
	}
}

func TestRateLimiterRefillsOverTime(t *testing.T) {
	rl := newRateLimiter(1, 10*time.Millisecond)

	if !rl.Allow("alice") {
		t.Fatal("first request should be allowed")
	}
	if rl.Allow("alice") {
		t.Fatal("second immediate request should be blocked")
	}
	time.Sleep(25 * time.Millisecond)
	if !rl.Allow("alice") {
		t.Error("request after refill interval should be allowed")
	}
}
