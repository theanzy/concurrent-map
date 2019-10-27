package cmap

import mapset "github.com/deckarep/golang-set"

// NestedGSet CMap(<Gset>) key<set1>,key<set2>
type NestedGSet struct {
	_cmap ConcurrentMap
}

// NewNestedGSet CMap(<Gset>) key<set1>,key<set2>
func NewNestedGSet() *NestedGSet {
	m := new(NestedGSet)
	m._cmap = New()
	return m
}

// SetGSetKey set inner key into inner set <key(Cmap), interkey(gset)>
// lock shard for outer key
// false if gset already has key
func (m *NestedGSet) SetGSetKey(key string, innerKey interface{}) bool {
	outerShard := m._cmap.GetShard(key) //cmap
	outerShard.Lock()
	defer outerShard.Unlock()
	// whether item was added
	isnew := false
	innerVal, ok := outerShard.items[key] // gset
	if ok {                               // get gset
		// cmap already exist for <key, innerKey>
		mySet, okSet := innerVal.(*mapset.Set)
		if okSet {
			isnew = (*mySet).Add(innerKey)
			// fmt.Println("SetGSetKey", isnew, innerKey)
		}
	} else {
		// key or innerkey not exist
		mySet := mapset.NewThreadUnsafeSet()
		isnew = mySet.Add(innerKey) // create new set
		// set new gset
		// fmt.Println("SetGSetKey", isnew, innerKey)
		outerShard.items[key] = &mySet
	}
	return isnew
}

// SetGSetMultiKeys set lists of inner keys into inner gset
// lock shard for outer key
// <key, CMap[InnerKey][val]>
func (m *NestedGSet) SetGSetMultiKeys(key string, innerKeys []interface{}) {
	shard := m._cmap.GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	// get innerCmap
	innerVal, ok := shard.items[key]
	if ok {
		// cmap already exist <key, innerKey>
		// get existing inner cmap
		mySet, okConv := innerVal.(*mapset.Set)
		if okConv {
			// set inner keys into cmap
			for _, innerKey := range innerKeys {
				(*mySet).Add(innerKey)
			}
		}
	} else {
		// key or innerkey not exist
		mySet := mapset.NewThreadUnsafeSet() // create new ConcurrentMap
		// set keys into new ConcurrentMap
		for _, innerKey := range innerKeys {
			mySet.Add(innerKey)
		}
		// set new inner cmap for key
		shard.items[key] = &mySet
	}
}

// HasGSetKey check if inner gset has inner key
func (m *NestedGSet) HasGSetKey(key string, innerKey interface{}) bool {
	val, ok := m._cmap.Get(key)
	if ok {
		shard := m._cmap.GetShard(key)
		shard.RLock()
		defer shard.RUnlock()
		mySet, okSet := val.(*mapset.Set)
		if okSet {
			return (*mySet).Contains(innerKey)
		}
	}
	return false
}

// DeleteGSetKey delete one innerkey in inner CMap of outer key
// lock shard for outer key.
//  Clear empty cmap
func (m *NestedGSet) DeleteGSetKey(key string, innerKey interface{}) {

	if val, ok := m._cmap.Get(key); ok {
		shard := m._cmap.GetShard(key)
		shard.Lock()
		defer shard.Unlock()
		mySet, okSet := val.(*mapset.Set)
		if okSet {
			(*mySet).Remove(innerKey)

			if (*mySet).Cardinality() == 0 {
				delete(shard.items, key)
			}
		}
	}
}

// DeleteGSetMultiKeys delete list of innerkeys in inner Gset of given outer key
// lock shard for outer key
func (m *NestedGSet) DeleteGSetMultiKeys(key string, innerKeys []interface{}) {

	if val, ok := m._cmap.Get(key); ok { // cmap exist for key
		shard := m._cmap.GetShard(key)
		shard.Lock()
		defer shard.Unlock()
		mySet, okSet := val.(*mapset.Set)
		if okSet {
			for _, innerKey := range innerKeys {
				(*mySet).Remove(innerKey)
			}
			if (*mySet).Cardinality() == 0 {
				delete(shard.items, key)
			}
		}
	}
}

// GetGSet returns inner gset for key
func (m *NestedGSet) GetGSet(key string) (*mapset.Set, bool) {
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
	mySet, okSet := val.(*mapset.Set)

	return mySet, okSet
}

// GetGSetNoLock returns inner gset for key
func (m *NestedGSet) GetGSetNoLock(key string) (*mapset.Set, bool) {
	// read lock
	// Get shard
	shard := m._cmap.GetShard(key)
	// Get item from shard for given key.
	val, exist := shard.items[key]
	if !exist { // outer key does not exists
		return nil, false
	}
	mySet, okSet := val.(*mapset.Set)

	return mySet, okSet
}

// GetGSetKeys Get list of inner keys in inner gset
// <Keys, <[k1]val1, [k2]val2>> get k1,k2, ...
func (m *NestedGSet) GetGSetKeys(key string) ([]interface{}, bool) {
	shard := m._cmap.GetShard(key)
	shard.RLock()
	defer shard.RUnlock()
	if mySet, exist := m.GetGSet(key); exist {
		results := make([]interface{}, 0, (*mySet).Cardinality())
		for iter := range (*mySet).Iter() {
			k := iter
			results = append(results, k)
		}
		return results, exist
	}
	return nil, false
}

// GetGSetStrKeys Get list of inner keys in inner gset
// <Keys, <[k1]val1, [k2]val2>> get k1,k2, ...
func (m *NestedGSet) GetGSetStrKeys(key string) ([]string, bool) {

	if mySet, exist := m.GetGSet(key); exist {
		shard := m._cmap.GetShard(key)
		shard.RLock()
		defer shard.RUnlock()
		results := make([]string, 0, (*mySet).Cardinality())
		for iter := range (*mySet).Iter() {
			if str, ok := iter.(string); ok {
				results = append(results, str)
			}
		}
		return results, exist
	}
	return nil, false
}
