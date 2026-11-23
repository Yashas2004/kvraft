package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func buildTestSSTable(tb testing.TB, n int) *SSTable {
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%08d", 2*i))
		if i%10 == 0 {
			entries = append(entries, Entry{Key: key, Deleted: true})
		} else {
			entries = append(entries, Entry{Key: key, Value: []byte(fmt.Sprintf("value%d", i))})
		}
	}

	path := filepath.Join(tb.TempDir(), "000001.sst")
	if err := WriteSSTable(path, entries); err != nil {
		tb.Fatal(err)
	}
	table, err := OpenSSTable(path)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { table.Close() })
	return table
}

func TestSSTableGet(t *testing.T) {
	n := 1000
	table := buildTestSSTable(t, n)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%08d", 2*i))
		value, deleted, found, err := table.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("%s not found", key)
		}
		if i%10 == 0 {
			if !deleted {
				t.Fatalf("%s should be a tombstone", key)
			}
		} else if deleted || string(value) != fmt.Sprintf("value%d", i) {
			t.Fatalf("%s: wrong value %q", key, value)
		}
	}

	absent := []string{"aaa", "zzz", "key00000001", "key00000999", "key99999999"}
	for _, key := range absent {
		_, _, found, err := table.lookup([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("%s should be absent", key)
		}
	}
}
