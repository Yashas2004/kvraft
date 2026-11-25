package storage

import (
	"bytes"
	"container/heap"
)

// entryIterator is anything that yields entries in key order:
// an SSTable (sstIterator) or an in-memory sorted slice (sliceIterator).
type entryIterator interface {
	Next() bool
	Current() Entry
	Err() error
}

// sliceIterator walks an already-sorted slice, e.g. the memtable's Entries().
type sliceIterator struct {
	entries []Entry
	pos     int
}

func (it *sliceIterator) Next() bool {
	if it.pos >= len(it.entries) {
		return false
	}
	it.pos++
	return true
}

func (it *sliceIterator) Current() Entry { return it.entries[it.pos-1] }
func (it *sliceIterator) Err() error     { return nil }

type heapItem struct {
	entry  Entry
	source int // index into the iterators slice; 0 = newest
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0 // smaller key first
	}
	return h[i].source < h[j].source // same key: newer source first
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// mergeTables does a k-way merge of sorted iterators (ordered newest first),
// keeping only the newest version of each key. Tombstones are dropped only
// if dropTombstones is true, which is safe only when no older data exists below.
func mergeTables(iterators []entryIterator, dropTombstones bool) ([]Entry, error) {
	h := &mergeHeap{}

	for i, it := range iterators {
		if it.Next() {
			heap.Push(h, heapItem{entry: it.Current(), source: i})
		} else if it.Err() != nil {
			return nil, it.Err()
		}
	}

	var output []Entry
	var lastKey []byte
	haveLast := false

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		it := iterators[item.source]
		if it.Next() {
			heap.Push(h, heapItem{entry: it.Current(), source: item.source})
		} else if it.Err() != nil {
			return nil, it.Err()
		}

		if haveLast && bytes.Equal(item.entry.Key, lastKey) {
			continue // older version of a key we already emitted
		}
		lastKey = item.entry.Key
		haveLast = true

		if item.entry.Deleted && dropTombstones {
			continue
		}
		output = append(output, item.entry)
	}
	return output, nil
}
