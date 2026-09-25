package api

import (
	"sync"
	"time"
)

// rateLimiter is a simple per-owner token bucket, so a single API key
// can never flood the scheduler's queue. Stdlib only, no external
// dependency.
type rateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     int
	interval time.Duration
}

type bucket struct {
	tokens   int
	lastFill time.Time
}

func newRateLimiter(ratePerInterval int, interval time.Duration) *rateLimiter {
	return &rateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     ratePerInterval,
		interval: interval,
	}
}

// Allow reports whether owner may proceed now, consuming one token if so.
func (rl *rateLimiter) Allow(owner string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[owner]
	if !ok {
		b = &bucket{tokens: rl.rate, lastFill: time.Now()}
		rl.buckets[owner] = b
	}

	if elapsed := time.Since(b.lastFill); elapsed >= rl.interval {
		refills := int(elapsed / rl.interval)
		b.tokens = min(b.tokens+refills*rl.rate, rl.rate)
		b.lastFill = b.lastFill.Add(time.Duration(refills) * rl.interval)
	}

	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}
