package cmap

import (
	pq "github.com/jupp0r/go-priority-queue"
)

// NestedQueue Cmap(key,<queue>)
type NestedQueue struct {
	_cmap ConcurrentMap
}

// NewNestedQueue returns Cmap(key,<queue>)
func NewNestedQueue() *NestedQueue {
	m := new(NestedQueue)
	m._cmap = New()
	return m
}

// IsEmpty return true if cmap empty
func (m *NestedQueue) IsEmpty() bool {
	return m._cmap.IsEmpty()
}

// Has return true if cmap has key
func (m *NestedQueue) Has(key string) bool {
	return m._cmap.Has(key)
}

// Keys returns all keys in cmap
func (m *NestedQueue) Keys() []string {
	return m._cmap.Keys()
}

// Count returns number of elements
func (m *NestedQueue) Count() int {
	return m._cmap.Count()
}

// Remove key in cmap
func (m *NestedQueue) Remove(key string) {
	m._cmap.Remove(key)
}

// InsertpQueue key, ({topic, payloads}, ...), priority
func (m *NestedQueue) InsertpQueue(key string, value interface{}, priority float64) bool {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()
	// whether item was added
	isnew := false
	// get queue
	innerVal, ok := outerShard.items[key]
	if ok {
		// cmap already exist for <key, innerKey>
		oldPQ, okpq := innerVal.(*pq.PriorityQueue)
		if okpq {
			(*oldPQ).Insert(value, priority)
		}
	} else {
		// key or innerkey not exist
		newPQ := pq.New()
		// fmt.Println("InsertpQueue", value)
		newPQ.Insert(value, priority) // create new set
		// set new gset
		outerShard.items[key] = &newPQ
		isnew = true
	}
	return isnew
}

// PopQueue pop queue for key
func (m *NestedQueue) PopQueue(key string) (interface{}, bool) {

	val, ok := m._cmap.Get(key)

	if ok {
		shard := m._cmap.GetShard(key)
		shard.Lock()
		defer shard.Unlock()
		oldPQ, okpq := val.(*pq.PriorityQueue)
		if okpq {
			v, err := oldPQ.Pop()

			if err != nil {
				return nil, false
			}
			if oldPQ.Len() == 0 {
				delete(shard.items, key)
			}
			return v, true
		}
	}
	return nil, false
}

// PopQueueStr pop queue for key
func (m *NestedQueue) PopQueueStr(key string) (string, bool) {

	val, ok := m._cmap.Get(key)

	if ok {
		shard := m._cmap.GetShard(key)
		shard.Lock()
		defer shard.Unlock()
		oldPQ, okpq := val.(*pq.PriorityQueue)
		if okpq {
			v, err := oldPQ.Pop()

			if err != nil {
				return "", false
			}
			if oldPQ.Len() == 0 {
				delete(shard.items, key)
			}
			if str, ok := v.(string); ok {
				return str, true
			}
		}
	}
	return "", false
}

// PopQueueBytes pop queue for key
func (m *NestedQueue) PopQueueBytes(key string) ([]byte, bool) {
	if result, ok := m.PopQueueStr(key); ok {
		return []byte(result), true
	}
	return nil, false
}
