package cmap

// NestedCMap Cmap with NestedGSet as values CMap<NestedGSet>
type NestedCMap struct {
	_cmap ConcurrentMap
}

// NewNestedCMap return NestedCMap
func NewNestedCMap() *NestedCMap {
	m := new(NestedCMap)
	m._cmap = New()
	return m
}

// IsEmpty return true if cmap empty
func (m *NestedCMap) IsEmpty() bool {
	return m._cmap.IsEmpty()
}

// Has return true if cmap has key
func (m *NestedCMap) Has(key string) bool {
	return m._cmap.Has(key)
}

// Keys returns all keys in cmap
func (m *NestedCMap) Keys() []string {
	return m._cmap.Keys()
}

// Count returns number of elements
func (m *NestedCMap) Count() int {
	return m._cmap.Count()
}

// Remove key in cmap
func (m *NestedCMap) Remove(key string) {
	m._cmap.Remove(key)
}

// Set key in cmap
func (m *NestedCMap) Set(key string, value interface{}) {
	m._cmap.Set(key, value)
}

// Pop key in cmap and return (value, isexist bool)
func (m *NestedCMap) Pop(key string) (interface{}, bool) {
	return m._cmap.Pop(key)
}

// SetInnerKey set inner key into inner CMap <key, ListofKeys>
// lock shard for outer key
// CMap<key, NestedGSet<InnerKey[vals,...]>>
func (m *NestedCMap) SetInnerKey(key, innerKey string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	// get innerCmap
	if nestedGSetVal, exist := outerShard.items[key]; exist { // NestedGSet already exist for <key>
		if nestedGSet, okNesetedGSet := nestedGSetVal.(*NestedGSet); okNesetedGSet { // convert
			nestedGSet._cmap.SetNoLock(innerKey, struct{}{}) // set innerkey for nested gset
		}
	} else { // nestedGset not exist for key
		nestedGSet := NewNestedGSet()                    // create new NestedGSet
		nestedGSet._cmap.SetNoLock(innerKey, struct{}{}) // set innerKey in nestedGset
		outerShard.items[key] = nestedGSet               // set nestedGset in cmap for key
	}
}

// SetInnerKeyVal set inner key into inner NestedGSet (CMapOuter,cmap inner, gset (values))
// lock shard for outer key
// CMap<key, NestedGSet<InnerKey[vals,...]>>
func (m *NestedCMap) SetInnerKeyVal(key, innerKey, innerVal string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock() // lock
	defer outerShard.Unlock()
	if nestedGSetVal, exist := outerShard.items[key]; exist { // cmap already exist for <key, innerKey>
		// cmap already exist for <key, innerKey>
		if nestedGSet, okNestedGSet := nestedGSetVal.(*NestedGSet); okNestedGSet { // convert
			nestedGSet.SetValueNoLock(innerKey, innerVal)
		}
	} else {
		// key or innerkey not exist
		nestedGSet := NewNestedGSet()                 // create new NestedGSet
		nestedGSet.SetValueNoLock(innerKey, innerVal) // set inner key with struct
		outerShard.items[key] = nestedGSetVal         // set nestedGset in cmap for key
	}
}

// SetCMapKeyValNoLock set inner key into inner NestedGSet (CMapOuter,cmap inner, gset (values))
// CMap<key, NestedGSet<InnerKey[vals,...]>>
func (m *NestedCMap) SetInnerKeyValNoLock(key, innerKey, innerVal string) {
	outerShard := m._cmap.GetShard(key)
	if nestedGSetVal, exist := outerShard.items[key]; exist { // nested gest exist in cmap for key
		if nestedGSet, okGSet := nestedGSetVal.(*NestedGSet); okGSet { // convert
			nestedGSet.SetValueNoLock(innerKey, innerVal) // set innerval in nestedGset for innerkey
		}
	} else {
		// key or innerkey not exist
		nestedGSet := NewNestedGSet()                 // create new NestedGSet
		nestedGSet.SetValueNoLock(innerKey, innerVal) // set innerVal to nested gset for innerKey
		outerShard.items[key] = nestedGSet            // set new nested gset for key
	}
}

// SetCMapMultiInnerKeys set lists of inner keys into inner NestedGSet
// lock shard for outer key
// CMap<key, NestedGSet<InnerKey[vals,...]>>
func (m *NestedCMap) SetMultiInnerKeys(key string, innerKeys []string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock() // lock
	defer outerShard.Unlock()
	if nestedGSetVal, exist := outerShard.items[key]; exist {
		if nestedGSet, okNestedGSet := nestedGSetVal.(*NestedGSet); okNestedGSet {
			for _, innerKey := range innerKeys {
				nestedGSet._cmap.SetNoLock(innerKey, struct{}{})
			}
		}
	} else { // key not exist in cmap
		nestedGSet := NewNestedGSet()        // create new nestedGset
		for _, innerKey := range innerKeys { // set innerkeys into new NestedGSet
			nestedGSet._cmap.SetNoLock(innerKey, struct{}{})
		}
		outerShard.items[key] = nestedGSet // set new nested gset in cmap for key
	}
}

// DeleteInnerKey delete one innerkey in inner NestedGSet for key
// lock shard for outer key.
//  Clear empty NestedGSet
func (m *NestedCMap) DeleteInnerKey(key, innerKey string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()
	if nestedGSetVal, exist := outerShard.items[key]; exist {
		if nestedGSet, okGSet := nestedGSetVal.(*NestedGSet); okGSet {
			nestedGSet._cmap.RemoveNoLock(innerKey)
			if nestedGSet.IsEmpty() {
				delete(outerShard.items, key)
			}
		}
	}
}

// DeleteInnerKeyNoLock delete one innerkey in inner NestedGSet for key
//  Clear empty NestedGSet
func (m *NestedCMap) DeleteInnerKeyNoLock(key, innerKey string) {
	outerShard := m._cmap.GetShard(key)
	if nestedGSetVal, exist := outerShard.items[key]; exist {
		if nestedGSet, okGSet := nestedGSetVal.(*NestedGSet); okGSet {
			nestedGSet._cmap.RemoveNoLock(innerKey)
			if nestedGSet.IsEmpty() {
				delete(outerShard.items, key)
			}
		}
	}
}

// DeleteInnerKeyVal delete innerKey in inner NestedGSet for key
// CMap<key, NestedGSet<InnerKey[vals,...]>>
func (m *NestedCMap) DeleteInnerKeyVal(key, innerKey, innerVal string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()
	if nestedGSetVal, exist := outerShard.items[key]; exist {
		// cmap already exist for <key, innerKey>
		if nestedGSet, okGSet := nestedGSetVal.(*NestedGSet); okGSet {
			nestedGSet.DeleteValueNoLock(innerKey, innerVal)
		}
	}
}

// DeleteInnerKeyValNoLock delete innerKey in inner NestedGSet for key
// lock shard for outer key
// CMap<key, NestedGSet<InnerKey[vals,...]>>
func (m *NestedCMap) DeleteInnerKeyValNoLock(key, innerKey, innerVal string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()
	if nestedGSetVal, exist := outerShard.items[key]; exist {
		// cmap already exist for <key, innerKey>
		if nestedGSet, okGSet := nestedGSetVal.(*NestedGSet); okGSet {
			nestedGSet.DeleteValueNoLock(innerKey, innerVal)
		}
	}
}

// DeleteInnerKeys delete list of innerkeys in inner NestedGSet for key
// lock shard for outer key
func (m *NestedCMap) DeleteInnerKeys(key string, innerKeys []string) {

	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	if nestedGSetVal, exist := outerShard.items[key]; exist { // cmap exist for key
		if nestedGSet, okNestedGSet := nestedGSetVal.(*NestedGSet); okNestedGSet {
			for _, innerKey := range innerKeys {
				nestedGSet._cmap.RemoveNoLock(innerKey)
			}
			if nestedGSet.IsEmpty() {
				delete(outerShard.items, key)
			}
		}
	}
}

// getCMap returns inner NestedGSet for key
func (m *NestedCMap) getNestedGSet(key string) (*NestedGSet, bool) {

	shard := m._cmap.GetShard(key)
	shard.RLock() // read lock
	defer shard.RUnlock()
	// Get item from shard for given key.
	if val, exist := shard.items[key]; exist {
		if nestedGSet, okNestedGSet := val.(*NestedGSet); okNestedGSet {
			if okNestedGSet {
				return nestedGSet, okNestedGSet
			}
		}
	}
	return nil, false
}

// getNestedGSetNoLock returns inner NestedGSet for key
func (m *NestedCMap) getNestedGSetNoLock(key string) (*NestedGSet, bool) {
	shard := m._cmap.GetShard(key)
	if val, exist := shard.items[key]; exist {
		if nestedGSet, okNestedGSet := val.(*NestedGSet); okNestedGSet {
			if okNestedGSet {
				return nestedGSet, okNestedGSet
			}
		}
	}
	return nil, false
}

// GetInnerKeys Get list of inner keys in inner NestedGset
// <Keys, <[k1]val1, [k2]val2>> get k1,k2, ...
func (m *NestedCMap) GetInnerKeys(key string) ([]string, bool) {
	shard := m._cmap.GetShard(key)
	shard.RLock()
	defer shard.RUnlock()
	if nestedGSet, exist := m.getNestedGSetNoLock(key); exist {
		return nestedGSet._cmap.Keys(), true
	}
	return nil, false
}

// GGetInnerKeysNoLock Get list of innerKeys in inner NestedGset
func (m *NestedCMap) GGetInnerKeysNoLock(key string) ([]string, bool) {
	if nestedGSet, exist := m.getNestedGSetNoLock(key); exist {
		return nestedGSet._cmap.Keys(), true
	}
	return nil, false
}

// GetInnerValues Get list of inner values in inner NestedGset
// CMap<key, NestedGSet<InnerKey[vals,...]>> get [vals,...] ...
func (m *NestedCMap) GetInnerValues(key, innerKey string) ([]string, bool) {
	outerShard := m._cmap.GetShard(key)
	outerShard.RLock()
	defer outerShard.RUnlock()
	// get innerCmap
	if nestedGsetVal, exist := outerShard.items[key]; exist {
		// cmap already exist for <key, innerKey>
		if nestedGSet, okCMap := nestedGsetVal.(*NestedGSet); okCMap {
			if result, okInnerKey := nestedGSet.GetStrValuesNoLock(innerKey); okInnerKey {
				return result, true
			}
		}
	}
	return nil, false
}

// GetInnerValuesNoLock Get list of inner values in inner NestedGset
// CMap<key, NestedGSet<InnerKey[vals,...]>> get [vals,...] ...
func (m *NestedCMap) GetInnerValuesNoLock(key, innerKey string) ([]string, bool) {
	outerShard := m._cmap.GetShard(key)
	// get innerCmap
	if nestedGsetVal, exist := outerShard.items[key]; exist {
		// cmap already exist for <key, innerKey>
		if nestedGSet, okCMap := nestedGsetVal.(*NestedGSet); okCMap {
			if result, okInnerKey := nestedGSet.GetStrValuesNoLock(innerKey); okInnerKey {
				return result, true
			}
		}
	}
	return nil, false
}
