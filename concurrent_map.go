package cmap

import (
	"encoding/json"
	"sync"
)

var SHARD_COUNT = 32

// ConcurrentMap is a "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMap []*ConcurrentMapShared

// ConcurrentMapShared is a "thread" safe string to anything map.
type ConcurrentMapShared struct {
	items        map[string]interface{}
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// New creates a new concurrent map.
func New() ConcurrentMap {
	m := make(ConcurrentMap, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		m[i] = &ConcurrentMapShared{items: make(map[string]interface{})}
	}
	return m
}

// GetShard returns shard under given key
func (m ConcurrentMap) GetShard(key string) *ConcurrentMapShared {
	return m[uint(fnv32(key))%uint(SHARD_COUNT)]
}

// MSet sets the given map to current maps.
func (m ConcurrentMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Set sets the given value under the specified key.
func (m ConcurrentMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// UpsertCb Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Upsert is Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetIfAbsent Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMap) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcurrentMap) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has Looks up an item under specified key
func (m ConcurrentMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m ConcurrentMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb func(key string, v interface{}, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMap) RemoveCb(key string, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m ConcurrentMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple is used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key string
	Val interface{}
}

// Iter returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot(m ConcurrentMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// Foreach shard.
	for index, shard := range m {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all items as map[string]interface{}
func (m ConcurrentMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// IterCb Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key string, v interface{})

// IterCb Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMap) IterCb(fn IterCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys as []string
func (m ConcurrentMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON Reviles ConcurrentMap "private" variables to json marshal.
func (m ConcurrentMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Concurrent map uses Interface{} as its value, therefor JSON Unmarshal
// will probably won't know which to type to unmarshal into, in such case
// we'll end up with a value of type map[string]interface{}, In most cases this isn't
// out value type, this is why we've decided to remove this functionality.

// func (m *ConcurrentMap) UnmarshalJSON(b []byte) (err error) {
// 	// Reverse process of Marshal.

// 	tmp := make(map[string]interface{})

// 	// Unmarshal into a single map.
// 	if err := json.Unmarshal(b, &tmp); err != nil {
// 		return nil
// 	}

// 	// foreach key,value pair in temporary map insert into our concurrent map.
// 	for key, val := range tmp {
// 		m.Set(key, val)
// 	}
// 	return nil
// }

/// ------------------- Mod ----------------------------------

// InsertOrIncrementKey set list inner key into CMap
// or increment value of innerkey if it exists
// lock shard for outer key
func (m ConcurrentMap) InsertOrIncrementKey(key string) int {
	shard := m.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	var result = 0
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(int); okInt {
			iCount = iCount + 1 // increment
			result = iCount
			shard.items[key] = iCount // update val
		}
	} else { // new entry
		result = 1
		shard.items[key] = result
	}
	return result
}

// InsertOrIncrementKeyNoLock set list inner key into CMap
// or increment value of innerkey if it exists
// lock shard for outer key
func (m ConcurrentMap) InsertOrIncrementKeyNoLock(key string) int {
	shard := m.GetShard(key)
	var result = 0
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(int); okInt {
			iCount = iCount + 1 // increment
			result = iCount
			shard.items[key] = iCount // update val
		}
	} else { // new entry
		result = 1
		shard.items[key] = result
	}
	return result
}

// InsertOrIncrementMultiKeys set list of keys <ListofKeys>
// or increment value of innerkey if it exists
// lock shard for outer key
func (m ConcurrentMap) InsertOrIncrementMultiKeys(keys []string) []int {
	var results []int
	for _, key := range keys {
		result := m.InsertOrIncrementKey(key) // lock each key
		results = append(results, result)
	}
	return results
}

// InsertOrIncrementMultiKeysNoLock set list of keys <ListofKeys>
// or increment value of innerkey if it exists
// lock shard for outer key
func (m ConcurrentMap) InsertOrIncrementMultiKeysNoLock(keys []string) []int {
	var results []int
	for _, key := range keys {
		result := m.InsertOrIncrementKeyNoLock(key) // lock each key
		results = append(results, result)
	}
	return results
}

// DecrementOrDeleteKey decrement value of key by one
// or delete one key in CMap if key count is zero
func (m ConcurrentMap) DecrementOrDeleteKey(key string) bool {
	var deleted = false

	shard := m.GetShard(key)
	shard.Lock()
	defer shard.Unlock()

	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(int); okInt {
			iCount = iCount - 1 // decrement val
			if iCount == 0 {
				// delete key entry from map
				delete(shard.items, key)
				deleted = true
			} else { // still have count
				// update decremented key
				shard.items[key] = iCount
			}
		}
	}

	return deleted
}

// DecrementOrDeleteKeyNoLock decrement value of key by one
// or delete one key in CMap if key count is zero
func (m ConcurrentMap) DecrementOrDeleteKeyNoLock(key string) bool {
	var deleted = false
	shard := m.GetShard(key)
	if val, exist := shard.items[key]; exist {
		if iCount, okInt := val.(int); okInt {
			iCount = iCount - 1 // decrement val
			if iCount == 0 {
				// delete  key
				delete(shard.items, key)
				deleted = true
			} else {
				// update decremented key
				shard.items[key] = iCount
			}
		}
	}
	return deleted
}

// DecrementOrDeleteMultiKeys decrement value of keys by one
// or delete one key in CMap if key count is zero
func (m ConcurrentMap) DecrementOrDeleteMultiKeys(keys []string) []string {

	var deletedKeys []string
	for _, key := range keys { // lock each key
		if deleted := m.DecrementOrDeleteKey(key); deleted {
			deletedKeys = append(deletedKeys, key)
		}
	}
	return deletedKeys
}

// SetNoLock sets the given value under the specified key without locking shard.
// If already lock for outer shard, locking inside will waste kernel resource
func (m ConcurrentMap) SetNoLock(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	// shard.Lock()
	shard.items[key] = value
	// shard.Unlock()
}

// RemoveNoLock removes an element from the map without locking shard.
// If already lock for outer shard, locking inside will waste kernel resource
func (m ConcurrentMap) RemoveNoLock(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	// shard.Lock()
	delete(shard.items, key)
	// return len(shard.items) > 0
	// shard.Unlock()
}
