package raft

import (
	"math/rand"
	"sync"
	"time"
)

// MemNetwork is an in-process network for tests. Nodes can be disconnected,
// split into partition groups, and links made unreliable (drops + delays).
type MemNetwork struct {
	mu         sync.Mutex
	nodes      []*Raft
	connected  []bool
	group      []int // nodes can talk only within the same group
	unreliable bool
}

func NewMemNetwork(n int) *MemNetwork {
	net := &MemNetwork{
		nodes:     make([]*Raft, n),
		connected: make([]bool, n),
		group:     make([]int, n),
	}
	for i := 0; i < n; i++ {
		net.connected[i] = true
	}
	return net
}

func (net *MemNetwork) SetNode(i int, rf *Raft) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[i] = rf
}

func (net *MemNetwork) SetConnected(i int, up bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.connected[i] = up
}

// Partition puts node i into group groups[i]; only same-group nodes can talk.
func (net *MemNetwork) Partition(groups []int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	copy(net.group, groups)
}

func (net *MemNetwork) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	for i := range net.group {
		net.group[i] = 0
	}
}

func (net *MemNetwork) SetUnreliable(on bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.unreliable = on
}

func (net *MemNetwork) Transport(from int) Transport {
	return &memTransport{net: net, from: from}
}

// link returns the target node, whether it is reachable, and whether links are unreliable.
func (net *MemNetwork) link(from, to int) (*Raft, bool, bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	ok := net.connected[from] && net.connected[to] && net.group[from] == net.group[to] && net.nodes[to] != nil
	return net.nodes[to], ok, net.unreliable
}

type memTransport struct {
	net  *MemNetwork
	from int
}

// deliver simulates one RPC: the request may be lost or delayed, the handler
// runs on the target, and the reply may be lost too.
func (t *memTransport) deliver(peer int, handle func(rf *Raft)) bool {
	rf, ok, unreliable := t.net.link(t.from, peer)
	if !ok {
		return false
	}
	if unreliable {
		if rand.Intn(10) == 0 {
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond) // caller waits, then gives up
			return false                                                // request lost
		}
		time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
	} else {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	}

	handle(rf)

	if _, ok, _ := t.net.link(t.from, peer); !ok {
		return false // link went down meanwhile: reply lost
	}
	if unreliable && rand.Intn(10) == 0 {
		return false // reply lost
	}
	return true
}

func (t *memTransport) RequestVote(peer int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return t.deliver(peer, func(rf *Raft) { rf.HandleRequestVote(args, reply) })
}

func (t *memTransport) AppendEntries(peer int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return t.deliver(peer, func(rf *Raft) { rf.HandleAppendEntries(args, reply) })
}

func (t *memTransport) InstallSnapshot(peer int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return t.deliver(peer, func(rf *Raft) { rf.HandleInstallSnapshot(args, reply) })
}
