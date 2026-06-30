package freehost

import "sync"

// failThreshold is the consecutive-failure count at which a provider is
// considered unhealthy and demoted in replica selection (still used as a
// last-resort candidate, never permanently removed — a host can recover).
const failThreshold = 3

// pool holds the enabled providers and their health, and orders candidate
// providers for replica selection. Health is a consecutive-failure counter fed
// by upload outcomes; a successful upload resets it.
type pool struct {
	mu        sync.Mutex
	providers []Provider // config priority order
	byName    map[string]Provider
	fails     map[string]int
	rr        int // round-robin rotation cursor
}

func newPool(providers []Provider) *pool {
	byName := make(map[string]Provider, len(providers))
	for _, p := range providers {
		byName[p.Name()] = p
	}
	return &pool{
		providers: providers,
		byName:    byName,
		fails:     map[string]int{},
	}
}

// get returns the provider with the given name (nil if not enabled).
func (p *pool) get(name string) Provider {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.byName[name]
}

// hasDurable reports whether any enabled provider is durable.
func (p *pool) hasDurable() bool {
	for _, pr := range p.providers {
		if pr.Durable() {
			return true
		}
	}
	return false
}

// candidates returns providers eligible to hold a chunkSize-byte chunk, ordered
// for selection: healthy-durable, healthy-temp, then unhealthy (durable, temp)
// as last resorts. Within each tier the order is rotated round-robin so load
// spreads across providers instead of always hammering the first one. The
// durable-first ordering means the first R candidates include a durable replica
// whenever a healthy durable exists (BUILD-PLAN §3 "always >= 1 durable").
func (p *pool) candidates(chunkSize int64) []Provider {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rr++
	rot := p.rr

	var healthyDur, healthyTmp, coldDur, coldTmp []Provider
	for _, pr := range p.providers {
		if pr.MaxBytes() < chunkSize {
			continue
		}
		healthy := p.fails[pr.Name()] < failThreshold
		switch {
		case healthy && pr.Durable():
			healthyDur = append(healthyDur, pr)
		case healthy:
			healthyTmp = append(healthyTmp, pr)
		case pr.Durable():
			coldDur = append(coldDur, pr)
		default:
			coldTmp = append(coldTmp, pr)
		}
	}
	rotate(healthyDur, rot)
	rotate(healthyTmp, rot)

	out := make([]Provider, 0, len(p.providers))
	out = append(out, healthyDur...)
	out = append(out, healthyTmp...)
	out = append(out, coldDur...)
	out = append(out, coldTmp...)
	return out
}

func (p *pool) markHealthy(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.fails, name)
}

func (p *pool) markFailed(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fails[name]++
}

// rotate left-rotates s in place by n positions (n may exceed len). A no-op for
// len <= 1.
func rotate(s []Provider, n int) {
	if len(s) <= 1 {
		return
	}
	n %= len(s)
	if n < 0 {
		n += len(s)
	}
	if n == 0 {
		return
	}
	tmp := append(append([]Provider(nil), s[n:]...), s[:n]...)
	copy(s, tmp)
}
