package internal

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type mockObject struct{}

func TestGetAndExists(t *testing.T) {
	factory := func() *mockObject {
		return &mockObject{}
	}
	cache, err := NewTTLCache(t.Context(), time.Hour, time.Minute, factory)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	s1 := cache.Get("key1")
	if s1 == nil {
		t.Fatal("Get returned nil")
	}

	s2 := cache.Get("key1")
	if s1 != s2 {
		t.Error("Get returned different instance for existing key")
	}

	if got, ok := cache.Exists("key1"); !ok {
		t.Error("Exists returned false for existing key")
	} else if got != s1 {
		t.Error("Exists returned different instance than Get")
	}

	if _, ok := cache.Exists("non-existent"); ok {
		t.Error("Exists returned true for non-existent key")
	}
}

func TestExpiration(t *testing.T) {
	ttl := 100 * time.Millisecond
	pruneInterval := ttl / 2
	factory := func() *mockObject { return &mockObject{} }

	cache, err := NewTTLCache(t.Context(), ttl, pruneInterval, factory)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	cache.Get("key1")
	key2 := cache.Get("key2")

	// Wait for ttl + pruneInterval + buffer, but keep refreshing key2
	for range 5 {
		time.Sleep(pruneInterval)
		cache.Get("key2")
	}

	if _, ok := cache.Exists("key1"); ok {
		t.Error("key1 should have been pruned")
	}
	if val, ok := cache.Exists("key2"); !ok {
		t.Error("key2 should not have been pruned")
	} else if val != key2 {
		t.Error("key2 returned different instance")
	}
}

func TestConcurrency(t *testing.T) {
	factory := func() *mockObject { return &mockObject{} }
	cache, _ := NewTTLCache(t.Context(), time.Hour, time.Minute, factory)
	var wg sync.WaitGroup

	// Store results for each iteration to verify consistency later
	results := make([]*mockObject, 100)

	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			s1 := cache.Get(key)
			s2, ok := cache.Exists(key)
			if !ok {
				t.Errorf("Exists returned false for key %s", key)
			}
			if s1 != s2 {
				t.Errorf("Exists returned different item from Get for key %s", key)
			}
			results[i] = s1
		}(i)
	}
	wg.Wait()

	// Verify that all results for the same key are identical
	for k := range 10 {
		expected := results[k]
		for i := 1; i < 10; i++ {
			idx := k + i*10
			if results[idx] != expected {
				t.Errorf("Key %d: instance at index %d differs from initial instance", k, idx)
			}
		}
	}
}
