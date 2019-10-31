package cmap

import (
	"sync"
	"sync/atomic"
)

// Uint64Map uses atomic uint64 as value for key
// CMap(key string, value uint64)
type Uint64Map struct {
	_cmap ConcurrentMap
	mtx   *sync.RWMutex
}

// IsEmpty return true if cmap empty
func (m *Uint64Map) IsEmpty() bool {
	return m._cmap.IsEmpty()
}

// Has return true if cmap has key
func (m *Uint64Map) Has(key string) bool {
	return m._cmap.Has(key)
}

// Keys returns all keys in cmap
func (m *Uint64Map) Keys() []string {
	return m._cmap.Keys()
}

// Count returns number of elements
func (m *Uint64Map) Count() int {
	return m._cmap.Count()
}

// Remove key in cmap
func (m *Uint64Map) Remove(key string) {
	m._cmap.Remove(key)
}

// Set key in cmap
func (m *Uint64Map) Set(key string, value interface{}) {
	m._cmap.Set(key, value)
}

// Pop key in cmap and return (value, isexist bool)
func (m *Uint64Map) Pop(key string) (interface{}, bool) {
	return m._cmap.Pop(key)
}

// NewUint64Map CMap(key string, value uint64)
func NewUint64Map() *Uint64Map {
	m := new(Uint64Map)
	m.mtx = new(sync.RWMutex)
	m._cmap = New()
	return m
}

// InsertOrIncrementKey set key into CMap or increment value of key if it exists
// lock shard for outer key
func (m *Uint64Map) InsertOrIncrementKey(key string) uint64 {
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(uint64); okInt {
			atomic.AddUint64(&iCount, 1)
			shard.items[key] = iCount // update val
			return iCount
		}
	} else { // new entry
		shard.items[key] = uint64(1)
		return uint64(1)
	}
	return 0
}

// InsertOrIncrementKeyNoLock set key into CMap or increment value of key if it exists
func (m *Uint64Map) InsertOrIncrementKeyNoLock(key string) uint64 {
	shard := m._cmap.GetShard(key)
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(uint64); okInt {
			atomic.AddUint64(&iCount, 1)
			shard.items[key] = iCount // update val
			return iCount
		}
	} else { // new entry
		shard.items[key] = uint64(1)
		return uint64(1)
	}
	return 0
}

// InsertOrIncrementMultiKeys list keys []string into CMap or increment value of key if it exists
// lock shard for outer key
func (m *Uint64Map) InsertOrIncrementMultiKeys(keys []string) []uint64 {
	var results []uint64
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for _, key := range keys {
		result := m.InsertOrIncrementKeyNoLock(key) // lock each key
		results = append(results, result)
	}
	return results
}

// InsertOrIncrementMultiKeysNoLock set list of keys <ListofKeys>
// or increment value of innerkey if it exists
func (m *Uint64Map) InsertOrIncrementMultiKeysNoLock(keys []string) []uint64 {
	var results []uint64
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for _, key := range keys {
		result := m.InsertOrIncrementKeyNoLock(key) // lock each key
		results = append(results, result)
	}
	return results
}

// DecrementOrDeleteKey decrement value of key by one
// or delete one key in CMap if key count is zero
func (m *Uint64Map) DecrementOrDeleteKey(key string) bool {
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(uint64); okInt {
			atomic.AddUint64(&iCount, ^uint64(0)) // decrement val
			if iCount == 0 {
				delete(shard.items, key)
				return true
			}
			// update decremented key
			shard.items[key] = iCount
		}
	}
	return false
}

// DecrementOrDeleteKeyNoLock decrement value of key by one
// or delete one key in CMap if key count is zero
func (m *Uint64Map) DecrementOrDeleteKeyNoLock(key string) bool {
	shard := m._cmap.GetShard(key)
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(uint64); okInt {
			atomic.AddUint64(&iCount, ^uint64(0)) // decrement val
			if iCount == 0 {
				delete(shard.items, key)
				return true
			}
			// update decremented key
			shard.items[key] = iCount
		}
	}
	return false
}

// DecrementOrDeleteMultiKeys decrement value of keys by one
// or delete one key in CMap if key count is zero
func (m *Uint64Map) DecrementOrDeleteMultiKeys(keys []string) []string {
	var deletedKeys []string
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for _, key := range keys { // lock each key
		if deleted := m.DecrementOrDeleteKeyNoLock(key); deleted {
			deletedKeys = append(deletedKeys, key)
		}
	}
	return deletedKeys
}

// DecrementOrDeleteMultiKeysNoLock decrement value of keys by one
// or delete one key in CMap if key count is zero
func (m *Uint64Map) DecrementOrDeleteMultiKeysNoLock(keys []string) []string {
	var deletedKeys []string
	for _, key := range keys { // lock each key
		if deleted := m.DecrementOrDeleteKeyNoLock(key); deleted {
			deletedKeys = append(deletedKeys, key)
		}
	}
	return deletedKeys
}
