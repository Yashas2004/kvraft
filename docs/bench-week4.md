# Week 4 — Raft leader election

Machine: Intel i5-13450HX, Windows 11. Heartbeat 50 ms, election timeout uniform 150–300 ms.

## Leader failover time (5 nodes, 50 leader failures, in-memory network)

Command: `go test ./raft/ -run=Distribution -v`

| Version           | min    | p50    | p90    | max    |
| ----------------- | ------ | ------ | ------ | ------ |
| Before rejoin fix | 32 ms  | 156 ms | 314 ms | 433 ms |
| After rejoin fix  | 115 ms | 139 ms | 188 ms | 490 ms |

- Floor ≈ 100 ms: minimum timeout (150) minus max time since last heartbeat (50).
- Expected p50 ≈ 155 ms: E[min of 4 Uniform(150,300)] = 150 + 150/5 = 180 ms, minus ~25 ms average heartbeat offset.
- min 32 ms before the fix exposed a bug: a rejoining ex-leader stepped down with a stale
  election timer, started an election instantly and deposed the healthy leader. Fixed by
  keeping the leader's timer fresh; regression test fails 3/3 without the fix, passes with it.
- max ~490 ms: split votes (two followers time out together, retry with new random timeouts).

## RPC round trip (TCP loopback, net/rpc + gob)

Command: `go test ./raft/ -run=none -bench=TCP -benchtime=3s`

- RequestVote round trip: 37 µs. Broadcast time ≪ election timeout (≈4000× margin), per Raft §5.6.
