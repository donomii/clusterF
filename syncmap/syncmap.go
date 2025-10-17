// A typed, thread-safe map built on top of sync.Map, using generics.
package syncmap

import "sync"

// SyncMap provides a thread-safe generic map built on top of sync.Map.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

// NewSyncMap creates a new thread-safe generic map
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{}
}

// Store sets a key-value pair
func (sm *SyncMap[K, V]) Store(key K, value V) {
	sm.m.Store(key, value)
}

// Load gets a value by key, returns value and whether it was found
func (sm *SyncMap[K, V]) Load(key K) (V, bool) {
	if val, ok := sm.m.Load(key); ok {
		return val.(V), true
	}
	var zero V
	return zero, false
}

// LoadOrStore gets existing value or stores new one, returns actual value and whether it was loaded
func (sm *SyncMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := sm.m.LoadOrStore(key, value)
	return actual.(V), loaded
}

// Delete removes a key
func (sm *SyncMap[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

// Range calls fn for each key-value pair.  Returning false quits the iteration
func (sm *SyncMap[K, V]) Range(fn func(key K, value V) bool) {
	sm.m.Range(func(key, value any) bool {
		return fn(key.(K), value.(V))
	})
}

// Keys returns all keys as a slice
func (sm *SyncMap[K, V]) Keys() []K {
	var keys []K
	sm.Range(func(key K, value V) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Values returns all values as a slice
func (sm *SyncMap[K, V]) Values() []V {
	var values []V
	sm.Range(func(key K, value V) bool {
		values = append(values, value)
		return true
	})
	return values
}

// Len returns the number of items (expensive operation)
func (sm *SyncMap[K, V]) Len() int {
	count := 0
	sm.Range(func(key K, value V) bool {
		count++
		return true
	})
	return count
}
