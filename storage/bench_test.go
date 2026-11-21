package storage

import (
	"fmt"
	"testing"
)

func benchmarkPut(b *testing.B, syncEachWrite bool) {
	db, err := OpenDB(b.TempDir(), syncEachWrite)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%09d", i)
		if err := db.Put([]byte(key), value); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/s")
}

func benchmarkBatch(b *testing.B, batchSize int) {
	db, err := OpenDB(b.TempDir(), true) // batches are always fsynced
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := make([]byte, 100)
	batch := make([]Entry, 0, batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%09d", i)
		batch = append(batch, Entry{Key: []byte(key), Value: value})
		if len(batch) == batchSize {
			if err := db.WriteBatch(batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		db.WriteBatch(batch)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/s")
}

func BenchmarkPutFsyncEachWrite(b *testing.B) { benchmarkPut(b, true) }
func BenchmarkPutNoFsync(b *testing.B)        { benchmarkPut(b, false) }
func BenchmarkBatch10(b *testing.B)           { benchmarkBatch(b, 10) }
func BenchmarkBatch100(b *testing.B)          { benchmarkBatch(b, 100) }
func BenchmarkBatch1000(b *testing.B)         { benchmarkBatch(b, 1000) }
