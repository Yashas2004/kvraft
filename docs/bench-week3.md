# Week 3 — Compaction: write vs. space amplification

Machine: Intel i5-13450HX, Windows 11.
Workload: 200,000 puts over 10,000 unique keys (~20 versions per key), 100 B values,
256 KB memtable, L0 trigger 4, level ratio 10, fsync off.
Command: `go test ./storage/ -run=Amplification -v`

| Metric                    | No compaction | With compaction | Change          |
| ------------------------- | ------------- | --------------- | --------------- |
| Flushes                   | 74            | 74              |                 |
| Compactions               | 0             | 18              |                 |
| L0 tables at end          | 74            | 2               |                 |
| Space amplification       | 19.31         | 1.63            | 11.8× less disk |
| Absent-key Get latency    | 10.2 µs       | 777 ns          | 13× faster      |
| Write amp (SSTables only) | 0.97          | 1.93            | ~2× more writes |
| Write amp (incl. WAL)     | 2.08          | 3.05            |                 |

## Definitions

- Write amplification = bytes written to disk / bytes written by the user.
- Space amplification = SSTable bytes on disk / live data bytes (10,000 × 111 B).

## Takeaways

- Compaction trades extra write I/O for faster reads and less disk: the core LSM trade-off.
- Space amp 19.31 ≈ 20 versions per key: without compaction every stale version stays on disk.
- Space amp 1.63, not 1.0: two L0 tables still hold stale copies, plus per-entry format
  overhead (9 B header, index, bloom).
- Write amp is only ~1.9 because the 1.1 MB dataset fits inside L1 (2.5 MB limit), so data
  is rewritten through one level only. Larger datasets cascade deeper; expected WA grows
  roughly with ratio × number of levels.
- SSTable write amp 0.97 < 1 without compaction because the final memtable was never flushed.
