package storage

import (
	"bytes"
	"container/heap"
)

type heapItem struct {
	entry  Entry
	source int // index into the iterators slice; 0 = newest table
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].entry.Key, h[j].entry.Key)
	if cmp != 0 {
		return cmp < 0 // smaller key first
	}
	return h[i].source < h[j].source // same key: newer table first
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
func mergeTables(iterators []*sstIterator, dropTombstones bool) ([]Entry, error) {
	h := &mergeHeap{}

	// Seed the heap with the first entry of every table.
	for i, it := range iterators {
		if it.Next() {
			heap.Push(h, heapItem{entry: it.current, source: i})
		} else if it.err != nil {
			return nil, it.err
		}
	}

	var output []Entry
	var lastKey []byte
	haveLast := false

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)

		// Refill from the table we just took from.
		it := iterators[item.source]
		if it.Next() {
			heap.Push(h, heapItem{entry: it.current, source: item.source})
		} else if it.err != nil {
			return nil, it.err
		}

		// Same key as the one we just emitted = an older version. Skip it.
		if haveLast && bytes.Equal(item.entry.Key, lastKey) {
			continue
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
