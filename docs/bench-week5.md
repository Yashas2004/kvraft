# Week 5 — Raft log replication + replicated KV

Machine: Intel i5-13450HX, Windows 11.
Setup: 3-node cluster over TCP loopback (net/rpc), each node with its own LSM storage engine.
Workload: N concurrent clients issuing 100 B Puts to the leader for 2 s per row.
Command: `go test ./kv/ -run=Replication -v -timeout=120s`

Caveat: the Raft log is not yet persisted (Week 6), so these are upper bounds.

## Results

| Clients | Before ops/s | After ops/s | After mean latency | Before p99 | After p99 |
| ------- | ------------ | ----------- | ------------------ | ---------- | --------- |
| 1       | 2,329        | 7,792       | 128 µs             | 6.1 ms     | 4.1 ms    |
| 4       | 3,664        | 23,516      | 170 µs             | 8.7 ms     | 5.3 ms    |
| 16      | 3,940        | 44,034      | 363 µs             | 23.9 ms    | 6.5 ms    |
| 64      | 2,791        | 57,860      | 1.1 ms             | 235 ms     | 9.5 ms    |

Peak throughput: 3,940 → 57,860 ops/s (14.7×). p99 at 64 clients: 235 → 9.5 ms (25× lower).

## Root cause of the "before" numbers

Every `Start()` launched its own AppendEntries to each peer, carrying all entries from
`nextIndex` onward. Since `nextIndex` only advances when a reply arrives, k concurrent
requests produced k overlapping RPCs each re-sending nearly the same entries: redundant
encoding, network and follower work grew roughly quadratically, so throughput fell as
clients increased.

## Fix

At most one AppendEntries in flight per peer. Requests arriving meanwhile set a `pending`
flag; when the RPC returns, one new RPC carries everything that accumulated. Batch size
grows with load automatically (stop-and-wait ARQ with batching). `pending` and `inFlight`
are checked under the same lock to avoid lost wakeups.

## Measurement note

Windows' clock ticks every ~0.5–1 ms, so sub-millisecond requests often measure as 0
(the original p50 column read "0s"). Mean latency is therefore computed with Little's Law:
mean latency = clients / throughput. p99 values are above the clock resolution and are
measured directly.
