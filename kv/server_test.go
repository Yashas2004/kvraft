package kv

import (
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Yashas2004/kvraft/raft"
	"github.com/Yashas2004/kvraft/storage"
)

func startKVCluster(t *testing.T, n int) []*Server {
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
	for i := 0; i < n; i++ {
		applyCh := make(chan raft.ApplyMsg)
		rf := raft.NewRaft(i, n, raft.NewTCPTransport(addrs), applyCh)
		if err := raft.Serve(rf, listeners[i]); err != nil {
			t.Fatal(err)
		}
		db, err := storage.OpenDB(t.TempDir(), false)
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = NewServer(rf, db, applyCh)
	}

	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			servers[i].rf.Kill()
			listeners[i].Close()
		}
		for i := 0; i < n; i++ {
			servers[i].db.Close() // before TempDir cleanup: Windows can't delete open files
		}
	})
	return servers
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
	servers := startKVCluster(t, 3)
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

	servers := startKVCluster(t, 3)
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
