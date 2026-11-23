# Week 2 — SSTable read benchmarks

Machine: Intel i5-13450HX, Windows 11. Table: 100,000 keys, file in OS page cache.
Command: `go test ./storage/ -run=none -bench=SSTable -benchtime=3s`

## Bloom filter accuracy (n = 10,000, target 1%)

- 9.6 bits/key, k = 7 hashes
- Theoretical FP: 1.004%, measured FP: 1.043% (100,000 absent probes)

## Read latency

| Lookup                 | ns/op |
| ---------------------- | ----- |
| present key            | 7,153 |
| absent key, no bloom   | 7,510 |
| absent key, with bloom | 268   |

Bloom cuts absent-key reads 28×. Check: ~190 ns hashing + 1% × 7,510 ns disk ≈ 265 ns.
