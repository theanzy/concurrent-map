package cmap

import mapset "github.com/deckarep/golang-set"

// NestedGSet CMap(<Gset>) ... key<set1>,key<set2>
type NestedGSet struct {
	_cmap ConcurrentMap
}

// NewNestedGSet CMap(<Gset>) key<set1>,key<set2>
func NewNestedGSet() *NestedGSet {
	m := new(NestedGSet)
	m._cmap = New()
	return m
}

// IsEmpty return true if cmap empty
func (m *NestedGSet) IsEmpty() bool {
	return m._cmap.IsEmpty()
}

// Has return true if cmap has key
func (m *NestedGSet) Has(key string) bool {
	return m._cmap.Has(key)
}

// Keys returns all keys in cmap
func (m *NestedGSet) Keys() []string {
	return m._cmap.Keys()
}

// Count returns number of elements
func (m *NestedGSet) Count() int {
	return m._cmap.Count()
}

// Remove key in cmap
func (m *NestedGSet) Remove(key string) {
	m._cmap.Remove(key)
}

// Set key in cmap
func (m *NestedGSet) Set(key string, value interface{}) {
	m._cmap.Set(key, value)
}

// SetGSet key in cmap
func (m *NestedGSet) SetGSet(key string, value *mapset.Set) {
	m._cmap.Set(key, value)
}

// Pop key in cmap and return (value, isexist bool)
func (m *NestedGSet) Pop(key string) (interface{}, bool) {
	return m._cmap.Pop(key)
}

// Set New Gset in Cmap for key
func setNewGset(shard *ConcurrentMapShared, key string, newSet *mapset.Set) {
	shard.items[key] = newSet
}

// SetValue set value into inner set <key(Cmap), interkey(gset)>
// lock shard for outer key
// false if gset already has key
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) SetValue(key string, value interface{}) bool {
	outerShard := m._cmap.GetShard(key) //cmap
	outerShard.Lock()
	defer outerShard.Unlock()
	// whether item was added
	if gsetVal, exist := outerShard.items[key]; exist { // get gset
		// cmap already exist for <key, value>
		if mySet, okSet := gsetVal.(*mapset.Set); okSet {
			return (*mySet).Add(value) // add value into gset
		}
	} else {
		// key not exist
		mySet := mapset.NewThreadUnsafeSet() // create new set
		isnew := mySet.Add(value)            // add value into gset
		setNewGset(outerShard, key, &mySet)  // set value to cmap
		return isnew
	}
	return false
}

// SetValueNoLock set inner key into inner set <key(Cmap), interkey(gset)>
// false if gset already has key
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) SetValueNoLock(key string, value interface{}) bool {
	outerShard := m._cmap.GetShard(key) //cmap
	// whether item was added
	if gsetVal, exist := outerShard.items[key]; exist { // get gset
		// cmap already exist for <key, innerKey>
		if mySet, okSet := gsetVal.(*mapset.Set); okSet {
			return (*mySet).Add(value) // add value into gset
		}
	} else { // key or innerkey not exist
		mySet := mapset.NewThreadUnsafeSet() // create new set
		isnew := mySet.Add(value)            // add value into gset
		setNewGset(outerShard, key, &mySet)  // set new gset to cmap
		return isnew
	}
	return false
}

// SetMultiValues set list of values into inner gset
// lock shard for outer key
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) SetMultiValues(key string, values []interface{}) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	if gsetVal, exist := outerShard.items[key]; exist { // GSet already exist in cmap for key
		// get existing inner cmap
		if mySet, okConv := gsetVal.(*mapset.Set); okConv {
			for _, innerKey := range values {
				(*mySet).Add(innerKey) // set value into Gset
			}
		}
	} else { // GSet dont exist for key
		// key or innerkey not exist
		mySet := mapset.NewThreadUnsafeSet() // create new ConcurrentMap
		// set keys into new ConcurrentMap
		for _, innerKey := range values {
			mySet.Add(innerKey) // set value into inner Gset
		}
		setNewGset(outerShard, key, &mySet) // set new gset for key
	}
}

// SetMultiValuesNoLock set list of values into inner gset
// lock shard for outer key
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) SetMultiValuesNoLock(key string, values []interface{}) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	if gsetVal, exist := outerShard.items[key]; exist { // GSet already exist in cmap for key
		// get existing inner cmap
		if mySet, okConv := gsetVal.(*mapset.Set); okConv {
			for _, innerKey := range values {
				(*mySet).Add(innerKey) // set value into Gset
			}
		}
	} else { // GSet dont exist for key
		// key or innerkey not exist
		mySet := mapset.NewThreadUnsafeSet() // create new ConcurrentMap
		// set keys into new ConcurrentMap
		for _, innerKey := range values {
			mySet.Add(innerKey) // set value into inner Gset
		}
		setNewGset(outerShard, key, &mySet) // set new gset for key
	}
}

// SetMultiStrValues set list of values into inner gset
// lock shard for outer key
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) SetMultiStrValues(key string, values []string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	if gsetVal, exist := outerShard.items[key]; exist { // GSet already exist for key
		// get existing inner cmap
		if mySet, okConv := gsetVal.(*mapset.Set); okConv {
			for _, value := range values {
				(*mySet).Add(value) // set value into inner Gset
			}
		}
	} else { // NestedGSet dont exist for key
		mySet := mapset.NewThreadUnsafeSet() // create new ConcurrentMap
		// set keys into new ConcurrentMap
		for _, value := range values {
			mySet.Add(value) // set value into inner Gset
		}
		setNewGset(outerShard, key, &mySet) // set new nested gset for key
	}
}

// SetMultiStrValuesNoLock set list of values into inner gset
// lock shard for outer key
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) SetMultiStrValuesNoLock(key string, values []string) {
	outerShard := m._cmap.GetShard(key)
	if gsetVal, exist := outerShard.items[key]; exist { // GSet already exist for key
		// get existing inner cmap
		if mySet, okConv := gsetVal.(*mapset.Set); okConv {
			for _, value := range values {
				(*mySet).Add(value) // set value into inner Gset
			}
		}
	} else { // NestedGSet dont exist for key
		mySet := mapset.NewThreadUnsafeSet() // create new ConcurrentMap
		// set keys into new ConcurrentMap
		for _, value := range values {
			mySet.Add(value) // set value into inner Gset
		}
		setNewGset(outerShard, key, &mySet) // set new nested gset for key
	}
}

// HasValue (locked) check if inner gset has value
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) HasValue(key string, value interface{}) bool {
	outerShard := m._cmap.GetShard(key)
	outerShard.RLock() // lock
	defer outerShard.RUnlock()
	if gsetVal, exist := outerShard.items[key]; exist { // GSet already exist in cmap for key
		if mySet, okConv := gsetVal.(*mapset.Set); okConv {
			return (*mySet).Contains(value) // true if set contains value
		}
	}
	return false
}

// HasValueNoLock check if inner gset has value
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) HasValueNoLock(key string, value interface{}) bool {
	outerShard := m._cmap.GetShard(key)
	if gsetVal, exist := outerShard.items[key]; exist { // GSet already exist in cmap for key
		if mySet, okConv := gsetVal.(*mapset.Set); okConv {
			return (*mySet).Contains(value) // true if set contains value
		}
	}
	return false
}

// DeleteValue (locked) delete one value in inner gset for key
// Clear empty cmap
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) DeleteValue(key string, value interface{}) {

	outerShard := m._cmap.GetShard(key)
	outerShard.Lock() // lock
	defer outerShard.Unlock()
	if gsetVal, exist := outerShard.items[key]; exist { // check if gset exist in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet {
			(*mySet).Remove(value) // remove value from gset for key
			if (*mySet).Cardinality() == 0 {
				delete(outerShard.items, key) // Clear empty set
			}
		}
	}
}

// DeleteValueNoLock delete one value in inner GSet of outer key
//  Clear empty gset
// CMap<key, GSet[val1,val2]>
func (m *NestedGSet) DeleteValueNoLock(key string, value interface{}) {

	outerShard := m._cmap.GetShard(key)
	if gsetVal, okVal := outerShard.items[key]; okVal {
		if mySet, okSet := gsetVal.(*mapset.Set); okSet {
			(*mySet).Remove(value) // remove value from gset for key
			if (*mySet).Cardinality() == 0 {
				delete(outerShard.items, key) // Clear empty set
			}
		}
	}
}

// DeleteMultipleValues (outer shard lock) delete list of values in inner Gset for key in cmap
// lock shard for outer key
func (m *NestedGSet) DeleteMultipleValues(key string, values []interface{}) {

	outerShard := m._cmap.GetShard(key)
	outerShard.Lock() // lock
	defer outerShard.Unlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exist in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert ok
			for _, value := range values {
				(*mySet).Remove(value) // remove value in inner GSet
			}
			if (*mySet).Cardinality() == 0 {
				delete(outerShard.items, key) // remove empty inner GSet
			}
		}
	}
}

// DeleteMultipleStrValues (outer shard lock) delete list of values in inner Gset for key in cmap
// lock shard for outer key
func (m *NestedGSet) DeleteMultipleStrValues(key string, values []string) {

	outerShard := m._cmap.GetShard(key)
	outerShard.Lock() // lock
	defer outerShard.Unlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exist in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert ok
			for _, value := range values {
				(*mySet).Remove(value) // remove value in inner GSet
			}
			if (*mySet).Cardinality() == 0 {
				delete(outerShard.items, key) // remove empty inner GSet
			}
		}
	}
}

// DeleteMultipleValuesNoLock delete list of values in inner Gset for key in cmap
// lock shard for outer key
func (m *NestedGSet) DeleteMultipleValuesNoLock(key string, values []interface{}) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock() // lock
	defer outerShard.Unlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exist in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert ok
			for _, value := range values {
				(*mySet).Remove(value) // remove value in inner GSet
			}
			if (*mySet).Cardinality() == 0 {
				delete(outerShard.items, key) // remove empty inner GSet
			}
		}
	}
}

// GetGSet returns inner gset in cmap for key
func (m *NestedGSet) GetGSet(key string) (*mapset.Set, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.RLock()
	defer outerShard.RUnlock()
	// Get item from shard for given key.
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			return mySet, true
		}
	}
	return nil, false
}

// GetGSetNoLock returns inner gset in cmap for key
func (m *NestedGSet) GetGSetNoLock(key string) (*mapset.Set, bool) {
	outerShard := m._cmap.GetShard(key)
	// Get item from shard for given key.
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			return mySet, true
		}
	}
	return nil, false
}

// PopGSet deletes key and returns inner gset in cmap for key
func (m *NestedGSet) PopGSet(key string) (*mapset.Set, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	outerShard.Unlock()
	// Get item from shard for given key.
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			delete(outerShard.items, key)
			return mySet, true
		}
	}
	return nil, false
}

// PopGSetNoLock deletes key and returns inner gset in cmap for key
func (m *NestedGSet) PopGSetNoLock(key string) (*mapset.Set, bool) {
	outerShard := m._cmap.GetShard(key)

	// Get item from shard for given key.
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			delete(outerShard.items, key)
			return mySet, true
		}
	}
	return nil, false
}

// PopStrValues deletes key and returns []string values of inner gset in cmap for key
func (m *NestedGSet) PopStrValues(key string) ([]string, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	outerShard.Unlock()
	// Get item from shard for given key.
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			results := make([]string, 0, (*mySet).Cardinality())
			for iter := range (*mySet).Iter() {
				if strResult, okStr := iter.(string); okStr {
					results = append(results, strResult)
				}
			}
			delete(outerShard.items, key)
			return results, true
		}
	}
	return nil, false
}

// PopStrValuesNoLock deletes key and returns []string values of inner gset in cmap for key
func (m *NestedGSet) PopStrValuesNoLock(key string) ([]string, bool) {
	outerShard := m._cmap.GetShard(key)
	// Get item from shard for given key.
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			results := make([]string, 0, (*mySet).Cardinality())
			for iter := range (*mySet).Iter() {
				if strResult, okStr := iter.(string); okStr {
					results = append(results, strResult)
				}
			}
			delete(outerShard.items, key)
			return results, true
		}
	}
	return nil, false
}

// GetValues Get list of values in inner gset for key
// CMap<[<key, GSet1[val1,val2]>, <key2, GSet2[val1,val2]>,... ] > , get val1, val2 for key
func (m *NestedGSet) GetValues(key string) ([]interface{}, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.RLock()
	defer outerShard.RUnlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet {
			return (*mySet).ToSlice(), true // return slice of values in gset for key
		}
	}
	return nil, false
}

// GetValuesNoLock Get list of values in inner gset for key
// CMap<[<key, GSet1[val1,val2]>, <key2, GSet2[val1,val2]>,... ] > , get val1, val2 for key
func (m *NestedGSet) GetValuesNoLock(key string) ([]interface{}, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.RLock()
	defer outerShard.RUnlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet {
			return (*mySet).ToSlice(), true // return slice of values in gset for key
		}
	}
	return nil, false
}

// GetStrValues Get list of values in inner gset for key
// CMap<[<key, GSet1[val1,val2]>, <key2, GSet2[val1,val2]>,... ] > , get val1, val2 for key
func (m *NestedGSet) GetStrValues(key string) ([]string, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.RLock()
	defer outerShard.RUnlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			result := make([]string, 0, (*mySet).Cardinality())
			for val := range (*mySet).Iter() {
				if strVal, strOK := val.(string); strOK {
					result = append(result, strVal)
				}
			}
			return result, true // return slice of values in gset for key
		} // for
	}
	return nil, false
}

// GetStrValuesNoLock Get list of values in inner gset for key
// CMap<[<key, GSet1[val1,val2]>, <key2, GSet2[val1,val2]>,... ] > , get val1, val2 for key
func (m *NestedGSet) GetStrValuesNoLock(key string) ([]string, bool) {
	outerShard := m._cmap.GetShard(key)
	// Get item from shard for given key.
	outerShard.RLock()
	defer outerShard.RUnlock()
	if gsetVal, exist := outerShard.items[key]; exist { // inner gset exists in cmap for key
		if mySet, okSet := gsetVal.(*mapset.Set); okSet { // convert
			result := make([]string, 0, (*mySet).Cardinality())
			for val := range (*mySet).Iter() {
				if strVal, strOK := val.(string); strOK {
					result = append(result, strVal)
				}
			} // for
			return result, true // return slice of values in gset for key
		}
	}
	return nil, false
}
