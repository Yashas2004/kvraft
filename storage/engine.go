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

const (
	defaultMemtableSizeLimit   = 4 * 1024 * 1024 // 4 MB
	defaultL0CompactionTrigger = 4
	defaultLevelSizeRatio      = 10
	numLevels                  = 7 // L0 .. L6
)

type Options struct {
	SyncEachWrite       bool
	MemtableSizeLimit   int  // flush once the memtable reaches this many bytes
	L0CompactionTrigger int  // compact L0 into L1 once L0 has this many tables
	LevelSizeRatio      int  // each level may be this many times bigger than the one above
	DisableCompaction   bool // for benchmarks: see what happens without compaction
}

type Stats struct {
	UserBytes       int64 // key+value bytes the user asked us to write
	WALBytes        int64
	FlushBytes      int64
	CompactionBytes int64
	Flushes         int
	Compactions     int
}

// WriteAmplification = total bytes written to disk / bytes the user wrote.
func (s Stats) WriteAmplification() float64 {
	if s.UserBytes == 0 {
		return 0
	}
	return float64(s.WALBytes+s.FlushBytes+s.CompactionBytes) / float64(s.UserBytes)
}

type tableFile struct {
	table *SSTable
	level int
	num   int
	size  int64
}

type fileID struct {
	level int
	num   int
}

type DB struct {
	mu          sync.RWMutex // writers: Lock; readers: RLock
	dir         string
	opts        Options
	wal         *WAL
	mem         *Memtable
	l0          []*tableFile          // newest first; key ranges may overlap
	levels      [numLevels]*tableFile // levels[1..6]: one sorted run each; levels[0] unused
	nextFileNum int
	stats       Stats
}

func OpenDB(dir string, syncEachWrite bool) (*DB, error) {
	return OpenDBWithOptions(dir, Options{SyncEachWrite: syncEachWrite})
}

func OpenDBWithOptions(dir string, opts Options) (*DB, error) {
	if opts.MemtableSizeLimit <= 0 {
		opts.MemtableSizeLimit = defaultMemtableSizeLimit
	}
	if opts.L0CompactionTrigger <= 0 {
		opts.L0CompactionTrigger = defaultL0CompactionTrigger
	}
	if opts.LevelSizeRatio <= 1 {
		opts.LevelSizeRatio = defaultLevelSizeRatio
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db := &DB{dir: dir, opts: opts, nextFileNum: 1}
	if err := db.loadTables(); err != nil {
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

func (db *DB) tablePath(level, num int) string {
	return filepath.Join(db.dir, fmt.Sprintf("L%d-%06d.sst", level, num))
}

// parseTableName turns "L1-000003.sst" into fileID{level: 1, num: 3}.
func parseTableName(name string) (fileID, bool) {
	if !strings.HasPrefix(name, "L") || !strings.HasSuffix(name, ".sst") {
		return fileID{}, false
	}
	trimmed := strings.TrimSuffix(strings.TrimPrefix(name, "L"), ".sst")
	parts := strings.Split(trimmed, "-")
	if len(parts) != 2 {
		return fileID{}, false
	}
	level, err := strconv.Atoi(parts[0])
	if err != nil || level < 0 || level >= numLevels {
		return fileID{}, false
	}
	num, err := strconv.Atoi(parts[1])
	if err != nil {
		return fileID{}, false
	}
	return fileID{level: level, num: num}, true
}

// obsoleteFiles finds files already consumed by a compaction that crashed
// before deleting its inputs. A file is obsolete if a newer-numbered file
// exists at a deeper level, or at the same level when that level is L1+.
func obsoleteFiles(files []fileID) map[fileID]bool {
	obsolete := make(map[fileID]bool)
	for _, f := range files {
		for _, other := range files {
			if other.num <= f.num {
				continue
			}
			if other.level > f.level || (other.level == f.level && f.level >= 1) {
				obsolete[f] = true
				break
			}
		}
	}
	return obsolete
}

func (db *DB) loadTables() error {
	dirEntries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var files []fileID
	for _, entry := range dirEntries {
		name := entry.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(db.dir, name)) // unfinished write, never valid
			continue
		}
		if id, ok := parseTableName(name); ok {
			files = append(files, id)
		}
	}

	obsolete := obsoleteFiles(files)
	for _, f := range files {
		if f.num >= db.nextFileNum {
			db.nextFileNum = f.num + 1
		}
		if obsolete[f] {
			os.Remove(db.tablePath(f.level, f.num))
			continue
		}
		tf, err := db.openTableFile(f.level, f.num)
		if err != nil {
			db.closeTables()
			return err
		}
		if f.level == 0 {
			db.l0 = append(db.l0, tf)
		} else {
			db.levels[f.level] = tf
		}
	}

	// L0 must be searched newest first.
	sort.Slice(db.l0, func(i, j int) bool { return db.l0[i].num > db.l0[j].num })
	return nil
}

func (db *DB) openTableFile(level, num int) (*tableFile, error) {
	path := db.tablePath(level, num)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	table, err := OpenSSTable(path)
	if err != nil {
		return nil, err
	}
	return &tableFile{table: table, level: level, num: num, size: info.Size()}, nil
}

func (db *DB) writeTableFile(level int, entries []Entry) (*tableFile, error) {
	num := db.nextFileNum
	db.nextFileNum++
	if err := WriteSSTable(db.tablePath(level, num), entries); err != nil {
		return nil, err
	}
	return db.openTableFile(level, num)
}

// deleteTableFile ignores errors on purpose: a file left behind is obsolete
// by the numbering rule and gets removed on the next open.
func (db *DB) deleteTableFile(tf *tableFile) {
	tf.table.Close() // Windows can't delete an open file
	os.Remove(db.tablePath(tf.level, tf.num))
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(Record{Type: recordPut, Key: key, Value: value}); err != nil {
		return err
	}
	db.mem.Put(key, value)
	db.stats.UserBytes += int64(len(key) + len(value))
	db.stats.WALBytes += int64(headerSize + len(key) + len(value))
	return db.maybeFlush()
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(Record{Type: recordDelete, Key: key}); err != nil {
		return err
	}
	db.mem.Delete(key)
	db.stats.UserBytes += int64(len(key))
	db.stats.WALBytes += int64(headerSize + len(key))
	return db.maybeFlush()
}

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
		db.stats.UserBytes += int64(len(e.Key) + len(e.Value))
		db.stats.WALBytes += int64(headerSize + len(e.Key) + len(e.Value))
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
	tf, err := db.writeTableFile(0, db.mem.Entries())
	if err != nil {
		return err
	}
	db.l0 = append([]*tableFile{tf}, db.l0...) // newest first
	db.stats.FlushBytes += tf.size
	db.stats.Flushes++

	// Data is safe in the SSTable, so the WAL can start over.
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

	if !db.opts.DisableCompaction && len(db.l0) >= db.opts.L0CompactionTrigger {
		return db.compact()
	}
	return nil
}

// levelLimit: L1 = memtable size x ratio, and each deeper level is ratio x bigger.
func (db *DB) levelLimit(level int) int64 {
	limit := int64(db.opts.MemtableSizeLimit) * int64(db.opts.LevelSizeRatio)
	for k := 2; k <= level; k++ {
		limit *= int64(db.opts.LevelSizeRatio)
	}
	return limit
}

// compactInto merges inputs (ordered newest first) into one new table at outputLevel,
// then deletes the inputs. The output is always written, even if empty, so the
// numbering rule can identify leftover inputs after a crash.
func (db *DB) compactInto(inputs []*tableFile, outputLevel int) (*tableFile, error) {
	var iterators []*sstIterator
	for _, in := range inputs {
		iterators = append(iterators, in.table.newIterator())
	}

	// Tombstones can only be dropped if no older data exists below the output.
	dropTombstones := true
	for k := outputLevel + 1; k < numLevels; k++ {
		if db.levels[k] != nil {
			dropTombstones = false
		}
	}

	merged, err := mergeTables(iterators, dropTombstones)
	if err != nil {
		return nil, err
	}
	output, err := db.writeTableFile(outputLevel, merged)
	if err != nil {
		return nil, err
	}
	db.stats.CompactionBytes += output.size
	db.stats.Compactions++

	// Only now, with the output safely on disk, remove the inputs.
	for _, in := range inputs {
		db.deleteTableFile(in)
	}
	return output, nil
}

func (db *DB) compact() error {
	// L0 (newest first) + L1 (older than every L0 table) -> new L1.
	inputs := make([]*tableFile, 0, len(db.l0)+1)
	inputs = append(inputs, db.l0...)
	if db.levels[1] != nil {
		inputs = append(inputs, db.levels[1])
	}
	output, err := db.compactInto(inputs, 1)
	if err != nil {
		return err
	}
	db.l0 = nil
	db.levels[1] = output

	// Cascade: push any oversized level down into the next one.
	for level := 1; level < numLevels-1; level++ {
		if db.levels[level] == nil || db.levels[level].size <= db.levelLimit(level) {
			break
		}
		inputs := []*tableFile{db.levels[level]}
		if db.levels[level+1] != nil {
			inputs = append(inputs, db.levels[level+1])
		}
		output, err := db.compactInto(inputs, level+1)
		if err != nil {
			return err
		}
		db.levels[level] = nil
		db.levels[level+1] = output
	}
	return nil
}

// Get: memtable, then L0 newest to oldest, then L1, L2, ... First hit wins.
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

	for _, tf := range db.l0 {
		value, deleted, found, err := tf.table.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if deleted {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	for level := 1; level < numLevels; level++ {
		tf := db.levels[level]
		if tf == nil {
			continue
		}
		value, deleted, found, err := tf.table.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if deleted {
				return nil, false, nil
			}
			return value, true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.stats
}

// TableBytes is the total size of all SSTables on disk (for space amplification).
func (db *DB) TableBytes() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var total int64
	for _, tf := range db.l0 {
		total += tf.size
	}
	for level := 1; level < numLevels; level++ {
		if db.levels[level] != nil {
			total += db.levels[level].size
		}
	}
	return total
}

func (db *DB) closeTables() {
	for _, tf := range db.l0 {
		tf.table.Close()
	}
	for level := 1; level < numLevels; level++ {
		if db.levels[level] != nil {
			db.levels[level].table.Close()
		}
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	err := db.wal.Close()
	db.closeTables()
	return err
}
