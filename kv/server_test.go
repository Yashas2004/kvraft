package kv

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yashas2004/kvraft/raft"
	"github.com/Yashas2004/kvraft/storage"
)

// startKVCluster returns the servers and each node's Raft directory.
func startKVCluster(t *testing.T, n, maxRaftLog int) ([]*Server, []string) {
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	servers := make([]*Server, n)
	persisters := make([]*raft.Persister, n)
	raftDirs := make([]string, n)
	for i := 0; i < n; i++ {
		applyCh := make(chan raft.ApplyMsg)
		raftDirs[i] = t.TempDir()
		persister, err := raft.OpenPersister(raftDirs[i])
		if err != nil {
			t.Fatal(err)
		}
		persisters[i] = persister
		rf := raft.NewRaft(i, n, raft.NewTCPTransport(addrs), applyCh, persister)
		if err := raft.Serve(rf, listeners[i]); err != nil {
			t.Fatal(err)
		}
		db, err := storage.OpenDB(t.TempDir(), false)
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = NewServer(rf, db, applyCh, maxRaftLog)
	}

	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			servers[i].Kill() // waits for any in-progress apply before we close the DB
			listeners[i].Close()
		}
		for i := 0; i < n; i++ {
			// Close files before TempDir cleanup: Windows can't delete open files.
			servers[i].db.Close()
			persisters[i].Close()
		}
	})
	return servers, raftDirs
}

func waitForKVLeader(t *testing.T, servers []*Server) int {
	for attempt := 0; attempt < 40; attempt++ {
		time.Sleep(50 * time.Millisecond)
		for i, s := range servers {
			if s.IsLeader() {
				return i
			}
		}
	}
	t.Fatalf("no leader")
	return -1
}

// do retries op on every server until one (the leader) succeeds.
func do(t *testing.T, servers []*Server, op func(s *Server) error) {
	for attempt := 0; attempt < 50; attempt++ {
		for _, s := range servers {
			if err := op(s); err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("operation never succeeded")
}

func TestKVBasic(t *testing.T) {
	servers, _ := startKVCluster(t, 3, 2) // tiny limit: every node snapshots constantly
	waitForKVLeader(t, servers)

	do(t, servers, func(s *Server) error { return s.Put([]byte("a"), []byte("1")) })
	do(t, servers, func(s *Server) error { return s.Put([]byte("b"), []byte("2")) })
	do(t, servers, func(s *Server) error { return s.Delete([]byte("a")) })

	var value []byte
	var found bool
	do(t, servers, func(s *Server) error {
		var err error
		value, found, err = s.Get([]byte("a"))
		return err
	})
	if found {
		t.Fatalf("a should be deleted, got %q", value)
	}

	do(t, servers, func(s *Server) error {
		var err error
		value, found, err = s.Get([]byte("b"))
		return err
	})
	if !found || string(value) != "2" {
		t.Fatalf("b: expected 2, got %q (found=%v)", value, found)
	}

	// Every replica's storage engine must converge to the same state.
	for i, s := range servers {
		converged := false
		for attempt := 0; attempt < 20; attempt++ {
			valueB, foundB, _ := s.db.Get([]byte("b"))
			_, foundA, _ := s.db.Get([]byte("a"))
			if foundB && string(valueB) == "2" && !foundA {
				converged = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !converged {
			t.Fatalf("replica %d did not converge", i)
		}
	}
}

func TestReportReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("slow measurement")
	}

	servers, _ := startKVCluster(t, 3, 0)
	leader := waitForKVLeader(t, servers)
	value := make([]byte, 100)
	duration := 2 * time.Second

	for _, clients := range []int{1, 4, 16, 64} {
		var mu sync.Mutex
		var latencies []time.Duration
		var wg sync.WaitGroup
		deadline := time.Now().Add(duration)

		for c := 0; c < clients; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				var local []time.Duration
				for i := 0; time.Now().Before(deadline); i++ {
					key := []byte(fmt.Sprintf("c%d-n%d-k%d", clients, c, i))
					start := time.Now()
					if err := servers[leader].Put(key, value); err != nil {
						t.Errorf("put failed: %v", err)
						return
					}
					local = append(local, time.Since(start))
				}
				mu.Lock()
				latencies = append(latencies, local...)
				mu.Unlock()
			}(c)
		}
		wg.Wait()

		if len(latencies) == 0 {
			t.Fatalf("no successful operations with %d clients", clients)
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		throughput := float64(len(latencies)) / duration.Seconds()
		p99 := latencies[len(latencies)*99/100]
		meanLatency := time.Duration(float64(clients) / throughput * float64(time.Second)) // Little's Law
		t.Logf("clients=%-3d  ops/s=%8.0f  mean=%-10v p99=%v", clients, throughput, meanLatency, p99)
	}
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

type restartCost struct {
	logBytes  int64
	snapBytes int64
	entries   int
	load      time.Duration // open persister + read raft-state, snapshot, log
	restore   time.Duration // open a storage engine + RestoreFrom(snapshot)
	replay    time.Duration // apply the log tail to the storage engine
}

// measureRestart does the work a restarting node does, timing each phase.
func measureRestart(t *testing.T, raftDir string) restartCost {
	cost := restartCost{
		logBytes:  fileSize(filepath.Join(raftDir, "raft-log")),
		snapBytes: fileSize(filepath.Join(raftDir, "raft-snapshot")),
	}

	start := time.Now()
	p, err := raft.OpenPersister(raftDir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := p.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	cost.entries = len(st.Log) - 1
	cost.load = time.Since(start)

	start = time.Now()
	db, err := storage.OpenDB(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Snapshot) > 0 {
		entries, _, err := decodeSnapshot(st.Snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RestoreFrom(entries); err != nil {
			t.Fatal(err)
		}
	}
	cost.restore = time.Since(start)

	start = time.Now()
	for _, e := range st.Log[1:] {
		op, _, _, key, value := decodeCommand(e.Command)
		if op == opPut {
			db.Put(key, value)
		} else if op == opDelete {
			db.Delete(key)
		}
	}
	cost.replay = time.Since(start)

	db.Close()
	p.Close()
	return cost
}

func TestReportSnapshots(t *testing.T) {
	if testing.Short() {
		t.Skip("slow measurement")
	}

	total := 20000
	uniqueKeys := 1000
	value := make([]byte, 100)

	for _, maxRaftLog := range []int{0, 1000, 5000, 10000} {
		servers, raftDirs := startKVCluster(t, 3, maxRaftLog)
		var leader atomic.Int64
		leader.Store(int64(waitForKVLeader(t, servers)))
		var retries atomic.Int64

		// put follows the leader like a real client: on failure, find the
		// current leader and retry. Retries > 0 means leadership moved.
		put := func(key []byte) error {
			for attempt := 0; attempt < 200; attempt++ {
				if err := servers[leader.Load()].Put(key, value); err == nil {
					return nil
				}
				retries.Add(1)
				for i, s := range servers {
					if s.IsLeader() {
						leader.Store(int64(i))
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
			return errors.New("put never succeeded")
		}

		var next atomic.Int64
		var wg sync.WaitGroup
		start := time.Now()
		for c := 0; c < 64; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := next.Add(1)
					if i > int64(total) {
						return
					}
					key := []byte(fmt.Sprintf("key%04d", i%int64(uniqueKeys)))
					if err := put(key); err != nil {
						t.Errorf("%v", err)
						return
					}
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		throughput := float64(total) / elapsed.Seconds()

		for _, s := range servers {
			s.Kill()
		}
		finalLeader := int(leader.Load())
		snapshots, scanTime, saveTime := servers[finalLeader].SnapshotStats()

		cost := measureRestart(t, raftDirs[finalLeader])
		label := "off"
		if maxRaftLog > 0 {
			label = fmt.Sprintf("every %d", maxRaftLog)
		}
		t.Logf("snapshots %-10s ops/s=%6.0f  workload=%v  client retries=%d", label, throughput, elapsed, retries.Load())
		t.Logf("   leader: %d snapshots, scan total %v, raft hand-off total %v", snapshots, scanTime, saveTime)
		t.Logf("   disk: raft-log %.1f KB, snapshot %.1f KB, %d log entries",
			float64(cost.logBytes)/1024, float64(cost.snapBytes)/1024, cost.entries)
		t.Logf("   restart: load %v + restore %v + replay %v = %v",
			cost.load, cost.restore, cost.replay, cost.load+cost.restore+cost.replay)
	}
}

// Five clients append numbered values while 30% of replies are "lost" AFTER
// the command was applied, forcing retries. Every value must appear exactly
// once, in order. Run with and without snapshots (dedup table must survive).
func TestClerkExactlyOnce(t *testing.T) {
	for _, maxRaftLog := range []int{0, 50} {
		servers, _ := startKVCluster(t, 3, maxRaftLog)
		waitForKVLeader(t, servers)
		for _, s := range servers {
			s.dropReplyProb = 0.3
		}

		clients := 5
		appends := 50
		var wg sync.WaitGroup
		for c := 0; c < clients; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				ck := NewClerk(servers)
				key := []byte(fmt.Sprintf("k-%d", c))
				for i := 0; i < appends; i++ {
					if err := ck.Append(key, []byte(fmt.Sprintf("x%d;", i))); err != nil {
						t.Errorf("client %d append %d: %v", c, i, err)
						return
					}
				}
			}(c)
		}
		wg.Wait()

		for _, s := range servers {
			s.dropReplyProb = 0
		}
		ck := NewClerk(servers)
		for c := 0; c < clients; c++ {
			expected := ""
			for i := 0; i < appends; i++ {
				expected += fmt.Sprintf("x%d;", i)
			}
			value, _, err := ck.Get([]byte(fmt.Sprintf("k-%d", c)))
			if err != nil {
				t.Fatal(err)
			}
			if string(value) != expected {
				t.Fatalf("maxRaftLog=%d client %d: expected %d bytes, got %d:\n%s",
					maxRaftLog, c, len(expected), len(value), value)
			}
		}
		t.Logf("maxRaftLog=%d: %d clients x %d appends with 30%% lost replies, all applied exactly once",
			maxRaftLog, clients, appends)
	}
}
