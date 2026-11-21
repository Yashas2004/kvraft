package storage

import (
	"bytes"
	"math/rand"
	"sync"
)

const maxLevel = 16 // enough for ~2^16 = 65k keys at full efficiency; still correct beyond

type node struct {
	key     []byte
	value   []byte
	deleted bool
	next    []*node // next[i] = next node in lane i
}

type Entry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

type Memtable struct {
	mu    sync.RWMutex
	head  *node // sentinel node, holds no key, is as tall as maxLevel
	level int   // number of lanes currently in use
	size  int   // approx bytes, used later to decide when to flush
	count int
}

func NewMemtable() *Memtable {
	head := &node{next: make([]*node, maxLevel)}
	return &Memtable{head: head, level: 1}
}

// randomLevel flips coins: keep growing while we get heads.
// P(level >= k) = (1/2)^(k-1)
func randomLevel() int {
	level := 1
	for level < maxLevel && rand.Intn(2) == 0 {
		level++
	}
	return level
}

func (m *Memtable) Put(key, value []byte) {
	m.insert(key, value, false)
}

func (m *Memtable) Delete(key []byte) {
	m.insert(key, nil, true)
}

func (m *Memtable) insert(key, value []byte, deleted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Copy so the caller can safely reuse their slices afterwards.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// update[i] = the last node in lane i whose key is < our key.
	// These are the nodes whose next pointers we will rewire.
	update := make([]*node, maxLevel)
	current := m.head
	for i := m.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, keyCopy) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	// Key already exists: overwrite in place.
	candidate := current.next[0]
	if candidate != nil && bytes.Equal(candidate.key, keyCopy) {
		m.size += len(valueCopy) - len(candidate.value)
		candidate.value = valueCopy
		candidate.deleted = deleted
		return
	}

	newLevel := randomLevel()
	if newLevel > m.level {
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}

	newNode := &node{
		key:     keyCopy,
		value:   valueCopy,
		deleted: deleted,
		next:    make([]*node, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	m.size += len(keyCopy) + len(valueCopy)
	m.count++
}

// Get returns (value, deleted, found).
// found=true, deleted=true means a tombstone: the key was deleted, stop searching.
func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	current := m.head
	for i := m.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}

	candidate := current.next[0]
	if candidate != nil && bytes.Equal(candidate.key, key) {
		return candidate.value, candidate.deleted, true
	}
	return nil, false, false
}

// Entries walks the bottom lane, which is every key in sorted order.
// Week 2 uses this to write the SSTable.
func (m *Memtable) Entries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]Entry, 0, m.count)
	current := m.head.next[0]
	for current != nil {
		entries = append(entries, Entry{Key: current.key, Value: current.value, Deleted: current.deleted})
		current = current.next[0]
	}
	return entries
}

func (m *Memtable) SizeBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}
