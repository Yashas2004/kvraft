package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALReplayWithTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	wal, _, err := OpenWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	records := []Record{
		{Type: recordPut, Key: []byte("a"), Value: []byte("1")},
		{Type: recordPut, Key: []byte("b"), Value: []byte("2")},
		{Type: recordDelete, Key: []byte("a")},
	}
	for _, r := range records {
		if err := wal.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	wal.Close()

	// Simulate a crash mid-write: half a record of garbage at the end.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{1, 2, 3, 4, 5, 6, 7})
	f.Close()

	wal2, replayed, err := OpenWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if len(replayed) != 3 {
		t.Fatalf("expected 3 records, got %d", len(replayed))
	}
	if string(replayed[1].Key) != "b" || string(replayed[1].Value) != "2" {
		t.Fatalf("record 1 wrong: %q=%q", replayed[1].Key, replayed[1].Value)
	}
	if replayed[2].Type != recordDelete {
		t.Fatalf("record 2 should be a delete")
	}
}
