# Week 6 — Persistence and snapshots

Machine: Intel i5-13450HX, Windows 11. 3 nodes, TCP loopback, 100 B values.

## 1. Durable replication (Raft log fsynced)

| Clients | fsync per Start (under lock) | Leader group commit | Change |
| ------- | ---------------------------- | ------------------- | ------ |
| 1       | 794 ops/s (1.26 ms)          | 1,278 (0.78 ms)     | 1.6×   |
| 4       | 1,620                        | 3,123               | 1.9×   |
| 16      | 1,812                        | 11,738              | 6.5×   |
| 64      | 1,706 (p99 135 ms)           | 40,187 (p99 7.8 ms) | 23.6×  |

Group commit: Start() appends in memory; a syncer persists all unsynced entries with one
fsync outside the Raft lock, overlapping the leader's disk write with replication
(Raft thesis §10.2.1). The leader counts itself toward a majority only once durable.

## 2. Snapshots / log compaction

Workload: 20,000 puts over 1,000 keys, 64 clients. Medians of 3 runs; 0 client retries in all runs.
Command: `go test ./kv/ -run=Snapshots -v -count=3`

| Snapshot interval | ops/s (median, range) | raft-log on disk       | snapshot | restart |
| ----------------- | --------------------- | ---------------------- | -------- | ------- |
| off               | 41,647 (34–46K)       | 2,676 KB (20,000 ent.) | –        | ~99 ms  |
| every 1,000       | 26,868 (12–28K)       | 104–331 KB             | 112 KB   | ~136 ms |
| every 5,000       | 12,131 (8–27K)        | 32 KB                  | 112 KB   | ~124 ms |
| every 10,000      | 19,000 (19–37K)       | 16 KB                  | 112 KB   | ~95 ms  |

Readings:

- The on-disk Raft log is bounded instead of growing with every write (8×–170× smaller here).
- Restart is a wash at this scale: history is only 20× the live data and replaying 20,000
  entries takes ~60 ms. Without snapshots restart grows with total history; with them it is
  bounded by live data.
- Throughput with snapshots varies 3–4× between identical runs on this laptop, so no clear
  interval trend can be claimed. Open item: profile disk contention from simultaneous
  raft-log compaction on all nodes.

## Bugs found by measurement

1. Snapshot scan flushed the memtable (file create/fsync/rename + compaction): 81 ms per
   snapshot. Fix: merge the memtable as one more sorted run: 0.23 ms (360×).
2. Snapshot save under the Raft lock took 169 ms on one run, more than the 150 ms election
   timeout, causing a spurious leader election and client errors. Fix: background snapshot
   saver; never do slow disk I/O on the leader's lock: 0 client retries since.
3. Async save broke an invariant (snapshot index ≤ locally durable index; followers alone
   can form a commit majority) → panic. Fix: never snapshot past the local durable index.
4. Shutdown races (DB closed mid-snapshot; compaction rename during measurement). Fix:
   KV Kill waits on an apply lock; Raft Kill joins the snapshot saver.
5. Declined snapshots re-scanned the DB every apply (1,251 scans instead of ~20). Fix:
   SnapshotReady check before scanning.
6. Windows rename fails with "Access is denied" while another handle is open (mandatory
   sharing modes vs Unix advisory locking). Fix: short retry with backoff.
