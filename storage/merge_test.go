package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func writeAndOpen(t *testing.T, dir, name string, entries []Entry) *SSTable {
	path := filepath.Join(dir, name)
	if err := WriteSSTable(path, entries); err != nil {
		t.Fatal(err)
	}
	table, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { table.Close() })
	return table
}

func TestMergeNewestWins(t *testing.T) {
	dir := t.TempDir()

	var oldest, middle, newest []Entry
	for i := 0; i < 100; i++ {
		oldest = append(oldest, Entry{Key: []byte(fmt.Sprintf("key%04d", i)), Value: []byte(fmt.Sprintf("old%d", i))})
	}
	for i := 50; i < 150; i++ {
		middle = append(middle, Entry{Key: []byte(fmt.Sprintf("key%04d", i)), Value: []byte(fmt.Sprintf("mid%d", i))})
	}
	for i := 0; i < 150; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		if i == 5 {
			newest = append(newest, Entry{Key: key, Deleted: true})
		} else if i%10 == 0 {
			newest = append(newest, Entry{Key: key, Value: []byte(fmt.Sprintf("new%d", i))})
		}
	}

	tables := []*SSTable{
		writeAndOpen(t, dir, "newest.sst", newest),
		writeAndOpen(t, dir, "middle.sst", middle),
		writeAndOpen(t, dir, "oldest.sst", oldest),
	}

	expectedValue := func(i int) string {
		if i%10 == 0 {
			return fmt.Sprintf("new%d", i)
		}
		if i >= 50 {
			return fmt.Sprintf("mid%d", i)
		}
		return fmt.Sprintf("old%d", i)
	}

	for _, dropTombstones := range []bool{false, true} {
		var iterators []entryIterator
		for _, table := range tables {
			iterators = append(iterators, table.newIterator())
		}

		merged, err := mergeTables(iterators, dropTombstones)
		if err != nil {
			t.Fatal(err)
		}

		expectedCount := 150
		if dropTombstones {
			expectedCount = 149
		}
		if len(merged) != expectedCount {
			t.Fatalf("drop=%v: expected %d entries, got %d", dropTombstones, expectedCount, len(merged))
		}

		for i := 1; i < len(merged); i++ {
			if bytes.Compare(merged[i-1].Key, merged[i].Key) >= 0 {
				t.Fatalf("output not strictly sorted at %d", i)
			}
		}

		for _, e := range merged {
			var i int
			fmt.Sscanf(string(e.Key), "key%04d", &i)
			if i == 5 {
				if dropTombstones || !e.Deleted {
					t.Fatalf("drop=%v: key 5 handled wrong", dropTombstones)
				}
				continue
			}
			if e.Deleted || string(e.Value) != expectedValue(i) {
				t.Fatalf("key %d: expected %s, got %q", i, expectedValue(i), e.Value)
			}
		}
	}
}
