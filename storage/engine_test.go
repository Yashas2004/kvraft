package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestEngineRestart(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Delete([]byte("a"))
	db.Put([]byte("c"), []byte("3"))
	db.Close()

	db2, err := OpenDB(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if _, found, _ := db2.Get([]byte("a")); found {
		t.Fatalf("a was deleted but came back")
	}
	value, found, _ := db2.Get([]byte("b"))
	if !found || string(value) != "2" {
		t.Fatalf("b: expected 2, got %q", value)
	}
	value, found, _ = db2.Get([]byte("c"))
	if !found || string(value) != "3" {
		t.Fatalf("c: expected 3, got %q", value)
	}
}

func TestEngineCrashRecovery(t *testing.T) {
	// When launched as the child, just write forever until killed.
	if os.Getenv("KV_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestEngineCrashRecovery")
	cmd.Env = append(os.Environ(), "KV_CRASH_CHILD=1", "KV_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	var ackedKeys []string
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "ACK ") {
			ackedKeys = append(ackedKeys, strings.TrimPrefix(line, "ACK "))
		}
		if len(ackedKeys) >= 200 {
			break
		}
	}

	cmd.Process.Kill() // hard kill, no cleanup runs in the child
	cmd.Wait()

	if len(ackedKeys) < 200 {
		t.Fatalf("child only acknowledged %d writes", len(ackedKeys))
	}

	db, err := OpenDB(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, key := range ackedKeys {
		if _, found, _ := db.Get([]byte(key)); !found {
			t.Fatalf("acknowledged key %s lost after crash", key)
		}
	}
	t.Logf("child acked %d writes before kill; all recovered", len(ackedKeys))
}

func runCrashChild() {
	db, err := OpenDB(os.Getenv("KV_DIR"), true)
	if err != nil {
		os.Exit(1)
	}
	for i := 0; ; i++ {
		key := fmt.Sprintf("key%06d", i)
		if err := db.Put([]byte(key), []byte("value")); err != nil {
			os.Exit(1)
		}
		fmt.Println("ACK " + key) // only printed after the write is durable
	}
}

func TestEngineFlushAndShadowing(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SyncEachWrite: false, MemtableSizeLimit: 1024}

	db, err := OpenDBWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	n := 2000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	for i := 0; i < n; i += 5 {
		db.Put([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("new%d", i)))
	}
	for i := 0; i < n; i += 7 {
		db.Delete([]byte(fmt.Sprintf("key%05d", i)))
	}

	stats := db.Stats()
	if stats.Flushes < 2 {
		t.Fatalf("expected several flushes, got %d", stats.Flushes)
	}
	t.Logf("%d flushes, %d compactions", stats.Flushes, stats.Compactions)

	check := func(db *DB) {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key%05d", i))
			value, found, err := db.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			if i%7 == 0 {
				if found {
					t.Fatalf("%s was deleted but found %q", key, value)
				}
			} else if i%5 == 0 {
				if !found || string(value) != fmt.Sprintf("new%d", i) {
					t.Fatalf("%s: expected new%d, got %q", key, i, value)
				}
			} else {
				if !found || string(value) != fmt.Sprintf("value%d", i) {
					t.Fatalf("%s: expected value%d, got %q", key, i, value)
				}
			}
		}
	}

	check(db)
	db.Close()

	db2, err := OpenDBWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	check(db2)
}

func TestObsoleteFiles(t *testing.T) {
	// Scenario: L0 #1 and #2 plus old L1 #0 were compacted into L1 #3,
	// crash before deleting inputs. Then L0 #4 was flushed. L2 #2 is older, deeper data.
	files := []fileID{
		{level: 1, num: 0},
		{level: 0, num: 1},
		{level: 0, num: 2},
		{level: 2, num: 2},
		{level: 1, num: 3},
		{level: 0, num: 4},
	}
	obsolete := obsoleteFiles(files)

	expectObsolete := []fileID{{1, 0}, {0, 1}, {0, 2}}
	expectKept := []fileID{{2, 2}, {1, 3}, {0, 4}}
	for _, f := range expectObsolete {
		if !obsolete[f] {
			t.Fatalf("L%d #%d should be obsolete", f.level, f.num)
		}
	}
	for _, f := range expectKept {
		if obsolete[f] {
			t.Fatalf("L%d #%d should be kept", f.level, f.num)
		}
	}
}

func TestEngineCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableSizeLimit: 1024, L0CompactionTrigger: 4, LevelSizeRatio: 4}

	db, err := OpenDBWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	n := 3000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	for i := 0; i < n; i += 3 {
		db.Put([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("new%d", i)))
	}
	for i := 0; i < n; i += 4 {
		db.Delete([]byte(fmt.Sprintf("key%05d", i)))
	}

	stats := db.Stats()
	if stats.Compactions == 0 {
		t.Fatalf("expected compactions to run")
	}
	if len(db.l0) >= opts.L0CompactionTrigger {
		t.Fatalf("L0 has %d tables, compaction should have kept it below %d", len(db.l0), opts.L0CompactionTrigger)
	}
	deepest := 0
	for level := 1; level < numLevels; level++ {
		if db.levels[level] != nil {
			deepest = level
			t.Logf("L%d: %d bytes (limit %d)", level, db.levels[level].size, db.levelLimit(level))
		}
	}
	if deepest < 2 {
		t.Fatalf("expected data to cascade to L2 or deeper, deepest is L%d", deepest)
	}
	t.Logf("%d flushes, %d compactions, L0 tables: %d", stats.Flushes, stats.Compactions, len(db.l0))

	check := func(db *DB) {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key%05d", i))
			value, found, err := db.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			if i%4 == 0 {
				if found {
					t.Fatalf("%s was deleted but found %q", key, value)
				}
			} else if i%3 == 0 {
				if !found || string(value) != fmt.Sprintf("new%d", i) {
					t.Fatalf("%s: expected new%d, got %q", key, i, value)
				}
			} else {
				if !found || string(value) != fmt.Sprintf("value%d", i) {
					t.Fatalf("%s: expected value%d, got %q", key, i, value)
				}
			}
		}
	}

	check(db)
	db.Close()

	db2, err := OpenDBWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	check(db2)
}

func TestReportAmplification(t *testing.T) {
	if testing.Short() {
		t.Skip("slow measurement")
	}

	uniqueKeys := 10000
	writes := 200000
	value := make([]byte, 100)
	liveBytes := int64(uniqueKeys * (11 + len(value))) // "key%08d" is 11 bytes

	for _, disable := range []bool{true, false} {
		db, err := OpenDBWithOptions(t.TempDir(), Options{MemtableSizeLimit: 256 * 1024, DisableCompaction: disable})
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < writes; i++ {
			key := []byte(fmt.Sprintf("key%08d", rand.Intn(uniqueKeys)))
			if err := db.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}

		lookups := 10000
		start := time.Now()
		for i := 0; i < lookups; i++ {
			db.Get([]byte(fmt.Sprintf("key%08d", uniqueKeys+i))) // never written
		}
		absentLatency := time.Since(start) / time.Duration(lookups)

		stats := db.Stats()
		lsmWA := float64(stats.FlushBytes+stats.CompactionBytes) / float64(stats.UserBytes)
		spaceAmp := float64(db.TableBytes()) / float64(liveBytes)

		t.Logf("compaction=%-5v flushes=%d compactions=%d L0 tables=%d", !disable, stats.Flushes, stats.Compactions, len(db.l0))
		t.Logf("   write amp: total %.2f (SSTables only %.2f)  space amp: %.2f  absent Get: %v",
			stats.WriteAmplification(), lsmWA, spaceAmp, absentLatency)
		db.Close()
	}
}

func TestScanAllAndRestore(t *testing.T) {
	opts := Options{MemtableSizeLimit: 1024}
	db, err := OpenDBWithOptions(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}

	expected := make(map[string]string)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key%04d", i%200)
		value := fmt.Sprintf("v%d", i)
		db.Put([]byte(key), []byte(value))
		expected[key] = value
	}
	for i := 0; i < 200; i += 3 {
		key := fmt.Sprintf("key%04d", i)
		db.Delete([]byte(key))
		delete(expected, key)
	}

	entries, err := db.ScanAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(expected) {
		t.Fatalf("ScanAll returned %d entries, expected %d", len(entries), len(expected))
	}
	for i, e := range entries {
		want, ok := expected[string(e.Key)]
		if !ok || want != string(e.Value) || e.Deleted {
			t.Fatalf("ScanAll entry %s=%q is wrong", e.Key, e.Value)
		}
		if i > 0 && bytes.Compare(entries[i-1].Key, e.Key) >= 0 {
			t.Fatalf("ScanAll not sorted at %d", i)
		}
	}
	db.Close()

	// Restore into a DB that already holds unrelated data: it must be replaced.
	dir2 := t.TempDir()
	db2, err := OpenDBWithOptions(dir2, opts)
	if err != nil {
		t.Fatal(err)
	}
	db2.Put([]byte("stale"), []byte("x"))
	if err := db2.RestoreFrom(entries); err != nil {
		t.Fatal(err)
	}

	check := func(d *DB) {
		for key, want := range expected {
			value, found, _ := d.Get([]byte(key))
			if !found || string(value) != want {
				t.Fatalf("%s: expected %s, got %q", key, want, value)
			}
		}
		for i := 0; i < 200; i += 3 {
			if _, found, _ := d.Get([]byte(fmt.Sprintf("key%04d", i))); found {
				t.Fatalf("deleted key%04d came back", i)
			}
		}
		if _, found, _ := d.Get([]byte("stale")); found {
			t.Fatalf("pre-restore data survived RestoreFrom")
		}
	}

	check(db2)
	db2.Close()

	db3, err := OpenDBWithOptions(dir2, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	check(db3)
}
