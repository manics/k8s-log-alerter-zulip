package internal

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ttlCacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

type TTLCache[V any] struct {
	cache   map[string]*ttlCacheEntry[V]
	mu      sync.Mutex
	ttl     time.Duration
	factory func() V
}

// pruneExpired locks the entire cache and deletes expired entries
func (c *TTLCache[V]) pruneExpired(ctx context.Context, pruneInterval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pruneInterval):
			now := time.Now()
			c.mu.Lock()
			for k, v := range c.cache {
				if v.expiresAt.Before(now) {
					delete(c.cache, k)
				}
			}
			c.mu.Unlock()
		}
	}
}

// NewTTLCache creates a new TTLCache
func NewTTLCache[V any](
	ctx context.Context,
	ttl time.Duration,
	pruneInterval time.Duration,
	factory func() V,
) (*TTLCache[V], error) {
	if ttl < 0 {
		return nil, fmt.Errorf("ttl:%d must be >0", ttl)
	}

	c := TTLCache[V]{
		cache:   make(map[string]*ttlCacheEntry[V]),
		ttl:     ttl,
		factory: factory,
	}

	go c.pruneExpired(ctx, pruneInterval)

	return &c, nil
}

// Exists returns an item if it exists, or the zero value and false
func (c *TTLCache[V]) Exists(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.cache[key]; ok {
		return entry.value, true
	}
	var zero V
	return zero, false
}

// Get returns a value, creating it if it doesn't exist
func (c *TTLCache[V]) Get(key string) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.cache[key]; ok {
		entry.expiresAt = time.Now().Add(c.ttl)
		return entry.value
	}
	val := c.factory()
	c.cache[key] = &ttlCacheEntry[V]{
		value:     val,
		expiresAt: time.Now().Add(c.ttl),
	}
	return val
}
