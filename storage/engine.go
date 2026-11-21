package storage

import (
	"os"
	"path/filepath"
	"sync"
)

type Engine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
	Close() error
}

var _ Engine = (*DB)(nil)

type DB struct {
	mu  sync.Mutex // makes "WAL append + memtable update" one atomic step
	wal *WAL
	mem *Memtable
}

func OpenDB(dir string, syncEachWrite bool) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	wal, records, err := OpenWAL(filepath.Join(dir, "wal.log"), syncEachWrite)
	if err != nil {
		return nil, err
	}

	// Recovery: redo every logged operation in order.
	mem := NewMemtable()
	for _, r := range records {
		if r.Type == recordPut {
			mem.Put(r.Key, r.Value)
		} else if r.Type == recordDelete {
			mem.Delete(r.Key)
		}
	}

	return &DB{wal: wal, mem: mem}, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write-ahead rule: log first, then apply.
	if err := db.wal.Append(Record{Type: recordPut, Key: key, Value: value}); err != nil {
		return err
	}
	db.mem.Put(key, value)
	return nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(Record{Type: recordDelete, Key: key}); err != nil {
		return err
	}
	db.mem.Delete(key)
	return nil
}

// WriteBatch logs all entries with one fsync, then applies them in order.
func (db *DB) WriteBatch(entries []Entry) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	records := make([]Record, 0, len(entries))
	for _, e := range entries {
		recordType := recordPut
		if e.Deleted {
			recordType = recordDelete
		}
		records = append(records, Record{Type: recordType, Key: e.Key, Value: e.Value})
	}

	if err := db.wal.AppendBatch(records); err != nil {
		return err
	}
	for _, e := range entries {
		if e.Deleted {
			db.mem.Delete(e.Key)
		} else {
			db.mem.Put(e.Key, e.Value)
		}
	}
	return nil
}

// Get returns (value, found, error). A tombstone counts as not found.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	value, deleted, found := db.mem.Get(key)
	if !found || deleted {
		return nil, false, nil
	}
	return value, true, nil
}

func (db *DB) Close() error {
	return db.wal.Close()
}
