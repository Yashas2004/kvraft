package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestMemtableBasic(t *testing.T) {
	m := NewMemtable()
	m.Put([]byte("banana"), []byte("2"))
	m.Put([]byte("apple"), []byte("1"))
	m.Put([]byte("cherry"), []byte("3"))
	m.Put([]byte("apple"), []byte("100")) // overwrite
	m.Delete([]byte("banana"))            // tombstone

	value, deleted, found := m.Get([]byte("apple"))
	if !found || deleted || string(value) != "100" {
		t.Fatalf("apple: got %q deleted=%v found=%v", value, deleted, found)
	}

	_, deleted, found = m.Get([]byte("banana"))
	if !found || !deleted {
		t.Fatalf("banana should be a tombstone")
	}

	_, _, found = m.Get([]byte("durian"))
	if found {
		t.Fatalf("durian should not exist")
	}

	entries := m.Entries()
	expected := []string{"apple", "banana", "cherry"}
	if len(entries) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(entries))
	}
	for i := 0; i < len(expected); i++ {
		if string(entries[i].Key) != expected[i] {
			t.Fatalf("entry %d: expected %s, got %s", i, expected[i], entries[i].Key)
		}
	}
}

func TestMemtableMatchesMap(t *testing.T) {
	m := NewMemtable()
	reference := make(map[string]string)

	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key%d", rand.Intn(1000))
		value := fmt.Sprintf("value%d", i)
		m.Put([]byte(key), []byte(value))
		reference[key] = value
	}

	for key, want := range reference {
		value, _, found := m.Get([]byte(key))
		if !found || string(value) != want {
			t.Fatalf("%s: expected %s, got %s", key, want, value)
		}
	}

	entries := m.Entries()
	if len(entries) != len(reference) {
		t.Fatalf("expected %d entries, got %d", len(reference), len(entries))
	}
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("not sorted at index %d", i)
		}
	}
}
