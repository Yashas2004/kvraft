# Week 7 — Correctness under chaos

## Exactly-once semantics

Client IDs + per-client sequence numbers + a replicated dedup table (included in snapshots).
Test: 5 clients × 50 appends while 30% of replies are dropped AFTER the command is applied.

- Without dedup: duplicates (e.g. client 0: 256 bytes instead of 190; "x4" applied 4 times).
- With dedup: every append exactly once, in order — with and without snapshots.

## Chaos harness

- 5 nodes on an in-memory network with: unreliable links (10% request loss, 10% reply loss,
  0–25 ms delay), random minority partitions, crash/restart of up to 2 nodes, snapshots.
- 5 clients, Put/Append/Get over 8 keys (one append-only), 5–15 ms think time.
- Two oracles after every run:
  1. Porcupine linearizability check of the client-visible history (per-key, since
     linearizability is local). Failed writes recorded as "maybe happened". A checker
     timeout counts as a failure.
  2. Replica convergence: once all replicas reach the same applied index, their storage
     engines must hold identical data.

Result: 50 runs (5 scenarios × 10), 0 linearizability violations, 0 divergences,
0 inconclusive checks. Checker time per run: 1–3 ms.
Command: `go test ./kv/ -run=Chaos -count=10`

## Bug found: double-applied appends after restart

A restarting node re-applied the Raft log onto a storage engine that already contained
those effects. Appends are not idempotent, so the restarted replica diverged
(append-only key: 6,170 bytes vs 3,883 on healthy replicas).

Why it was hard to see:

1. Porcupine passed: clients only read from the leader, never the corrupted follower.
2. Puts in the workload overwrote keys, so replay "self-healed" the corruption.

Exposed by adding the replica-convergence oracle and an append-only key.
Fix: the storage engine is a materialized view of Raft's snapshot + log — wipe it on
start and rebuild. (Future optimization: persist the applied index in the engine,
ARIES pageLSN-style, to skip already-applied entries.)

## Checker scalability

Linearizability checking is NP-complete. Unbounded client load produced ~6–10K ops per
run and the checker timed out. Bounding history size (think time, 8 keys) brought
checks from >60 s (timeout) to 1–3 ms.
