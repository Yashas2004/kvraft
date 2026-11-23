package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Engine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
	Close() error
}

var _ Engine = (*DB)(nil)

const defaultMemtableSizeLimit = 4 * 1024 * 1024 // 4 MB

type Options struct {
	SyncEachWrite     bool
	MemtableSizeLimit int // flush to an SSTable once the memtable reaches this many bytes
}

type DB struct {
	mu          sync.RWMutex // writers: Lock; readers: RLock
	dir         string
	opts        Options
	wal         *WAL
	mem         *Memtable
	tables      []*SSTable // newest first
	nextFileNum int
}

// OpenDB keeps the old signature so existing tests and benchmarks still work.
func OpenDB(dir string, syncEachWrite bool) (*DB, error) {
	return OpenDBWithOptions(dir, Options{SyncEachWrite: syncEachWrite})
}

func OpenDBWithOptions(dir string, opts Options) (*DB, error) {
	if opts.MemtableSizeLimit <= 0 {
		opts.MemtableSizeLimit = defaultMemtableSizeLimit
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db := &DB{dir: dir, opts: opts, nextFileNum: 1}
	if err := db.loadSSTables(); err != nil {
		return nil, err
	}

	wal, records, err := OpenWAL(db.walPath(), opts.SyncEachWrite)
	if err != nil {
		db.closeTables()
		return nil, err
	}

	// Recovery: redo every logged operation since the last flush.
	mem := NewMemtable()
	for _, r := range records {
		if r.Type == recordPut {
			mem.Put(r.Key, r.Value)
		} else if r.Type == recordDelete {
			mem.Delete(r.Key)
		}
	}

	db.wal = wal
	db.mem = mem
	return db, nil
}

func (db *DB) walPath() string {
	return filepath.Join(db.dir, "wal.log")
}

func (db *DB) sstPath(num int) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", num))
}

// loadSSTables opens every .sst file in the directory, newest (highest number) first.
func (db *DB) loadSSTables() error {
	dirEntries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var numbers []int
	for _, entry := range dirEntries {
		name := entry.Name()
		if strings.HasSuffix(name, ".tmp") {
			// A flush that crashed before its rename. Never complete, safe to delete.
			os.Remove(filepath.Join(db.dir, name))
			continue
		}
		if !strings.HasSuffix(name, ".sst") {
			continue
		}
		num, err := strconv.Atoi(strings.TrimSuffix(name, ".sst"))
		if err != nil {
			continue
		}
		numbers = append(numbers, num)
	}

	sort.Sort(sort.Reverse(sort.IntSlice(numbers)))

	for _, num := range numbers {
		table, err := OpenSSTable(db.sstPath(num))
		if err != nil {
			db.closeTables()
			return err
		}
		db.tables = append(db.tables, table)
		if num >= db.nextFileNum {
			db.nextFileNum = num + 1
		}
	}
	return nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write-ahead rule: log first, then apply.
	if err := db.wal.Append(Record{Type: recordPut, Key: key, Value: value}); err != nil {
		return err
	}
	db.mem.Put(key, value)
	return db.maybeFlush()
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(Record{Type: recordDelete, Key: key}); err != nil {
		return err
	}
	db.mem.Delete(key)
	return db.maybeFlush()
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
	return db.maybeFlush()
}

// maybeFlush must be called with db.mu held for writing.
func (db *DB) maybeFlush() error {
	if db.mem.SizeBytes() < db.opts.MemtableSizeLimit {
		return nil
	}
	return db.flush()
}

func (db *DB) flush() error {
	// Step 1: write the memtable to a new SSTable (atomic via .tmp + rename).
	num := db.nextFileNum
	path := db.sstPath(num)
	if err := WriteSSTable(path, db.mem.Entries()); err != nil {
		return err
	}
	table, err := OpenSSTable(path)
	if err != nil {
		return err
	}
	// Prepend: the newest table must be checked first.
	db.tables = append([]*SSTable{table}, db.tables...)
	db.nextFileNum++

	// Step 2: the data is safe on disk, so the WAL can start over.
	// Windows can't delete an open file, so close it first.
	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(db.walPath()); err != nil {
		return err
	}
	wal, _, err := OpenWAL(db.walPath(), db.opts.SyncEachWrite)
	if err != nil {
		return err
	}
	db.wal = wal
	db.mem = NewMemtable()
	return nil
}

// Get: memtable first, then SSTables newest to oldest. First hit wins.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	value, deleted, found := db.mem.Get(key)
	if found {
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}

	for _, table := range db.tables {
		value, deleted, found, err := table.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if deleted {
				return nil, false, nil // a newer tombstone hides older values
			}
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) closeTables() {
	for _, table := range db.tables {
		table.Close()
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	err := db.wal.Close()
	db.closeTables()
	return err
}
