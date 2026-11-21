# Week 1 — WAL write benchmarks

Machine: Intel i5-13450HX, Windows 11. Values: 100 B, keys: 12 B.
Command: `go test ./storage/ -run=none -bench=. -benchtime=3s` (single run)

| Mode                  | ns/op   | writes/s | vs fsync-per-write |
| --------------------- | ------- | -------- | ------------------ |
| fsync per write       | 683,195 | 1,464    | 1×                 |
| no fsync (unsafe)     | 19,021  | 52,676   | 36×                |
| batch 10, one fsync   | 239,138 | 4,184    | 2.9×               |
| batch 100, one fsync  | 16,535  | 60,512   | 41×                |
| batch 1000, one fsync | 6,172   | 162,037  | ~110×              |

## Takeaways

- fsync dominates write cost (~0.7 ms each); batching amortizes it.
- Batch-100 beats no-fsync: per-record write syscalls (~19 µs on Windows) cost more than one shared fsync.
- Crash safety: kill -9 subprocess test recovered 200/200 acknowledged writes.
