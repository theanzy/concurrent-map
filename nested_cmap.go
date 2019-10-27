package cmap

// NestedCMap Cmap with innerCmap as values
type NestedCMap struct {
	_cmap ConcurrentMap
}

// NewNestedCMap return NestedCMap
func NewNestedCMap() *NestedCMap {
	m := new(NestedCMap)
	m._cmap = New()
	return m
}

// SetCMapKey set inner key into inner CMap <key, ListofKeys>
// lock shard for outer key
// <key, CMap[InnerKey][val]>
func (m *NestedCMap) SetCMapKey(key, innerKey string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	// get innerCmap
	if innerVal, ok := outerShard.items[key]; ok {
		// cmap already exist for <key, innerKey>
		if innerCmap, okCMap := innerVal.(ConcurrentMap); okCMap {
			innerCmap.SetNoLock(innerKey, struct{}{})
		}
	} else {
		// key or innerkey not exist
		var innerCmap = New()                     // create new ConcurrentMap
		innerCmap.SetNoLock(innerKey, struct{}{}) // set inner key with struct
		// set new cmap
		outerShard.items[key] = innerCmap
	}
}

// SetCMapKeyVal set inner key into inner CMap <key, ListofKeys> (CMapOuter,cmap inner, gset (values))
// lock shard for outer key
// <key, CMap[InnerKey][val]>
func (m *NestedCMap) SetCMapKeyVal(key, innerKey, innerVal string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	// get innerCmap
	if innerGsetVal, ok := outerShard.items[key]; ok {
		// cmap already exist for <key, innerKey>
		if innerGset, okCMap := innerGsetVal.(*NestedGSet); okCMap {
			innerGset.SetGSetKey(innerKey, innerVal)
		}
	} else {
		// key or innerkey not exist
		var innerGset = NewNestedGSet()          // create new NestedGSet
		innerGset.SetGSetKey(innerKey, innerVal) // set inner key with struct
		// set new cmap
		outerShard.items[key] = innerGset
	}
}

// SetCMapMultiKeys set lists of inner keys into inner CMap
// lock shard for outer key
// <key, CMap[InnerKey][val]>
func (m *NestedCMap) SetCMapMultiKeys(key string, innerKeys []string) {
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	// get innerCmap
	if innerVal, ok := shard.items[key]; ok {
		// cmap already exist <key, innerKey>
		// get existing inner cmap
		if innerGset, okConv := innerVal.(*NestedGSet); okConv {
			// set inner keys into cmap
			for _, innerKey := range innerKeys {
				innerGset.SetGSetKey(innerKey, struct{}{})
			}
		}

	} else {
		// key or innerkey not exist
		var innerCmap = New() // create new ConcurrentMap
		// set keys into new ConcurrentMap
		for _, innerKey := range innerKeys {
			innerCmap.SetNoLock(innerKey, struct{}{})
		}
		// set new inner cmap for key
		shard.items[key] = innerCmap
	}
}

// InsertOrIncrementCMapKey set inner key into inner CMap <key, ListofKeys>
// or increment value of innerkey if it exists
// lock shard for outer key
func (m *NestedCMap) InsertOrIncrementCMapKey(key, innerKey string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()

	// get innerCMap
	innerVal, exist := outerShard.items[key]
	if exist { // already exist <key, innerKey>
		innerCmap := innerVal.(ConcurrentMap)
		innerCmap.InsertOrIncrementKeyNoLock(innerKey)

	} else { // key or innercmap not exist
		var innerCmap = New() // create new ConcurrentMap
		innerCmap.SetNoLock(innerKey, 1)
		// set new cmap for key
		outerShard.items[key] = innerCmap
	}
}

// InsertOrIncrementCMapMultiKeys set list of inner keys into inner CMap <key, ListofKeys>
// or increment value of innerkey if it exists
// lock shard for outer key
func (m *NestedCMap) InsertOrIncrementCMapMultiKeys(key string, innerKeys []string) {
	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()
	// get innerCmap
	innerVal, exist := outerShard.items[key]
	if exist { // already exist <key, innerKey>
		if innerCmap, okCMap := innerVal.(ConcurrentMap); okCMap {
			innerCmap.InsertOrIncrementMultiKeysNoLock(innerKeys) // this is lock
		}
		// innerCmap.Set(innerKey, struct{}{})
	} else { // key or innercmap not exist
		var innerCmap = New() // create new ConcurrentMap
		for _, innerKey := range innerKeys {
			innerCmap.SetNoLock(innerKey, 1)
		}
		// set new cmap for key
		outerShard.items[key] = innerCmap
	}
}

// DecrementOrDeleteCMapKey decrement value of innerkey by one
// or delete one innerkey in inner CMap of outer key if inner key count is zero
func (m *NestedCMap) DecrementOrDeleteCMapKey(key, innerKey string) bool {
	var deleted = false
	val, ok := m._cmap.Get(key)
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	if ok { // inner value exists for <key>
		if innerCmap, okCMap := val.(ConcurrentMap); okCMap { // inside inner cmap
			// innerCmap.Remove(innerKey)
			deleted = innerCmap.DecrementOrDeleteKeyNoLock(innerKey) // this is lock
			if innerCmap.IsEmpty() {
				delete(shard.items, key)
			}
		}
	}
	return deleted
}

// DecrementOrDeleteMultiCMapKeys decrement value of list of innerkeys by one
// or delete innerkeys in inner CMap of outer key if inner key count is zero
func (m *NestedCMap) DecrementOrDeleteMultiCMapKeys(key string, innerKeys []string) []string {
	var deletedInnerKeys []string
	val, ok := m._cmap.Get(key)
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	if ok { // inner value exists for <key>
		innerCmap, okCMap := val.(ConcurrentMap)
		if okCMap { // inside inner cmap
			deletedInnerKeys = innerCmap.DecrementOrDeleteMultiKeys(innerKeys)
		}
		if innerCmap.IsEmpty() {
			delete(shard.items, key)
		}
	}
	return deletedInnerKeys
}

// DeleteCMapKey delete one innerkey in inner CMap of outer key
// lock shard for outer key.
//  Clear empty cmap
func (m *NestedCMap) DeleteCMapKey(key, innerKey string) {

	val, ok := m._cmap.Get(key)
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	if ok {
		innerCmap, okCMap := val.(ConcurrentMap)
		if okCMap {
			innerCmap.RemoveNoLock(innerKey)

			if innerCmap.IsEmpty() {
				delete(shard.items, key)
			}
		}
	}
}

// DeleteCMapMultiKeys delete list of innerkeys in inner CMap of given outer key
// lock shard for outer key
func (m *NestedCMap) DeleteCMapMultiKeys(key string, innerKeys []string) {

	val, ok := m._cmap.Get(key)
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()

	if ok { // cmap exist for key
		innerCmap, okCnv := val.(ConcurrentMap)
		if okCnv {
			for _, innerKey := range innerKeys {
				innerCmap.RemoveNoLock(innerKey)
			}
			if innerCmap.IsEmpty() {
				delete(shard.items, key)
			}
		}
	}
}

// getCMap returns inner ConcurrentMap for key
func (m *NestedCMap) getCMap(key string) (ConcurrentMap, bool) {
	// read lock
	// Get shard
	shard := m._cmap.GetShard(key)
	shard.RLock()
	defer shard.RUnlock()
	// Get item from shard for given key.
	val, exist := shard.items[key]
	if !exist { // outer key does not exists
		return nil, false
	}
	innerCMap, okConv := val.(ConcurrentMap)

	return innerCMap, okConv
}

// getCMap returns inner ConcurrentMap for key
func (m *NestedCMap) getCMapNoLock(key string) (ConcurrentMap, bool) {
	// read lock
	// Get shard
	shard := m._cmap.GetShard(key)
	// Get item from shard for given key.
	val, exist := shard.items[key]
	if !exist { // outer key does not exists
		return nil, false
	}
	innerCMap, okConv := val.(ConcurrentMap)

	return innerCMap, okConv
}

// GetInnerCMapKeys Get list of inner keys in inner ConcurrentMap
// <Keys, <[k1]val1, [k2]val2>> get k1,k2, ...
func (m *NestedCMap) GetInnerCMapKeys(key string) ([]string, bool) {
	innerCmap, exist := m.getCMap(key)
	var results []string
	if !exist {
		results = nil
	} else {
		results = innerCmap.Keys()
	}
	return results, exist
}

// GetInnerCMapValues Get list of inner values in inner ConcurrentMap
// <Keys, <[k1]val1, [k2]val2>> get val1, val2, ...
func (m *NestedCMap) GetInnerCMapValues(key, innerKey string) ([]string, bool) {

	outerShard := m._cmap.GetShard(key)
	outerShard.Lock()
	defer outerShard.Unlock()
	// get innerCmap
	if innerGsetVal, ok := outerShard.items[key]; ok {
		// cmap already exist for <key, innerKey>
		if innerGset, okCMap := innerGsetVal.(*NestedGSet); okCMap {
			result, okInnerKey := innerGset.GetGSetStrKeys(innerKey)
			if okInnerKey {
				return result, true
			}
		}
	}
	return nil, false
}
