package raft

import (
	"net"
	"testing"
	"time"
)

func startTCPCluster(t *testing.T, n int) ([]*Raft, []net.Listener) {
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0") // port 0 = OS picks a free port
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	nodes := make([]*Raft, n)
	for i := 0; i < n; i++ {
		nodes[i] = NewRaft(i, n, NewTCPTransport(addrs), nil, nil)
		if err := Serve(nodes[i], listeners[i]); err != nil {
			t.Fatal(err)
		}
	}

	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			nodes[i].Kill()
			listeners[i].Close()
		}
	})
	return nodes, listeners
}

func waitForLeader(t *testing.T, nodes []*Raft, alive []bool) int {
	for attempt := 0; attempt < 40; attempt++ {
		time.Sleep(50 * time.Millisecond)

		leader := -1
		leaderTerm := -1
		for i, rf := range nodes {
			if !alive[i] {
				continue
			}
			term, isLeader := rf.GetState()
			if !isLeader {
				continue
			}
			if term == leaderTerm {
				t.Fatalf("two leaders in term %d", term)
			}
			if term > leaderTerm {
				leader = i
				leaderTerm = term
			}
		}
		if leader != -1 {
			return leader
		}
	}
	t.Fatalf("no leader elected over TCP")
	return -1
}

func TestTCPElectionAndFailover(t *testing.T) {
	nodes, listeners := startTCPCluster(t, 3)
	alive := []bool{true, true, true}

	leader := waitForLeader(t, nodes, alive)
	oldTerm, _ := nodes[leader].GetState()

	nodes[leader].Kill()
	listeners[leader].Close()
	alive[leader] = false

	newLeader := waitForLeader(t, nodes, alive)
	newTerm, _ := nodes[newLeader].GetState()
	if newLeader == leader || newTerm <= oldTerm {
		t.Fatalf("expected a new leader in a higher term, got node %d term %d", newLeader, newTerm)
	}
	t.Logf("leader %d (term %d) killed; node %d elected in term %d over TCP", leader, oldTerm, newLeader, newTerm)
}

func BenchmarkTCPRequestVoteRoundTrip(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addrs := []string{ln.Addr().String(), "127.0.0.1:1"} // node 1 doesn't exist; fine for this test

	rf := NewRaft(0, 2, NewTCPTransport(addrs), nil, nil)
	if err := Serve(rf, ln); err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	defer rf.Kill()

	client := NewTCPTransport(addrs)
	args := RequestVoteArgs{Term: 0, CandidateID: 1} // stale term: cheap reject, pure RPC cost

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var reply RequestVoteReply
		if !client.RequestVote(0, args, &reply) {
			b.Fatal("rpc failed")
		}
	}
}
