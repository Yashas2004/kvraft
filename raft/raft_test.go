package raft

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---------- in-memory network ----------

type network struct {
	mu        sync.Mutex
	nodes     []*Raft
	connected []bool
}

func (n *network) reachable(from, to int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.connected[from] && n.connected[to] && n.nodes[to] != nil
}

func (n *network) node(id int) *Raft {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[id]
}

type localTransport struct {
	net  *network
	from int
}

func (lt *localTransport) RequestVote(peer int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	if !lt.net.reachable(lt.from, peer) {
		return false
	}
	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond) // network delay
	lt.net.node(peer).HandleRequestVote(args, reply)
	return lt.net.reachable(lt.from, peer) // reply lost if the link went down meanwhile
}

func (lt *localTransport) AppendEntries(peer int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if !lt.net.reachable(lt.from, peer) {
		return false
	}
	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	lt.net.node(peer).HandleAppendEntries(args, reply)
	return lt.net.reachable(lt.from, peer)
}

// ---------- cluster helpers ----------

type cluster struct {
	t   *testing.T
	net *network
	n   int
}

func newCluster(t *testing.T, n int) *cluster {
	net := &network{nodes: make([]*Raft, n), connected: make([]bool, n)}
	for i := 0; i < n; i++ {
		net.connected[i] = true
	}
	for i := 0; i < n; i++ {
		rf := NewRaft(i, n, &localTransport{net: net, from: i})
		net.mu.Lock()
		net.nodes[i] = rf
		net.mu.Unlock()
	}
	t.Cleanup(func() {
		for _, rf := range net.nodes {
			rf.Kill()
		}
	})
	return &cluster{t: t, net: net, n: n}
}

func (c *cluster) setConnected(id int, up bool) {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	c.net.connected[id] = up
}

func (c *cluster) disconnect(id int) { c.setConnected(id, false) }
func (c *cluster) connect(id int)    { c.setConnected(id, true) }

func (c *cluster) isConnected(id int) bool {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.connected[id]
}

// checkOneLeader waits for a leader among connected nodes and fails if any
// term ever has two leaders. Returns the leader of the latest term.
func (c *cluster) checkOneLeader() int {
	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(50 * time.Millisecond)

		leadersByTerm := make(map[int][]int)
		for i := 0; i < c.n; i++ {
			if !c.isConnected(i) {
				continue
			}
			term, isLeader := c.net.node(i).GetState()
			if isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], i)
			}
		}

		latestTerm := -1
		for term, ids := range leadersByTerm {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			if term > latestTerm {
				latestTerm = term
			}
		}
		if latestTerm != -1 {
			return leadersByTerm[latestTerm][0]
		}
	}
	c.t.Fatalf("no leader elected")
	return -1
}

func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		if !c.isConnected(i) {
			continue
		}
		if _, isLeader := c.net.node(i).GetState(); isLeader {
			c.t.Fatalf("node %d is leader without a majority", i)
		}
	}
}

func (c *cluster) checkTermsAgree() int {
	term := -1
	for i := 0; i < c.n; i++ {
		if !c.isConnected(i) {
			continue
		}
		nodeTerm, _ := c.net.node(i).GetState()
		if term == -1 {
			term = nodeTerm
		} else if nodeTerm != term {
			c.t.Fatalf("nodes disagree on term: %d vs %d", term, nodeTerm)
		}
	}
	return term
}

func (c *cluster) newLeaderAbove(oldTerm int) bool {
	for i := 0; i < c.n; i++ {
		if !c.isConnected(i) {
			continue
		}
		term, isLeader := c.net.node(i).GetState()
		if isLeader && term > oldTerm {
			return true
		}
	}
	return false
}

// ---------- tests ----------

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()

	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTermsAgree()

	// With no failures, heartbeats should keep the same leader and term.
	time.Sleep(2 * electionTimeoutMax)
	term2 := c.checkTermsAgree()
	if term1 != term2 {
		t.Fatalf("term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	leader1 := c.checkOneLeader()

	// Leader fails: the other two must elect a new one.
	c.disconnect(leader1)
	c.checkOneLeader()

	// Old leader returns: it must step down, still exactly one leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Cut off a majority: the lone survivor must NOT become leader.
	other := (leader2 + 1) % 3
	c.disconnect(leader2)
	c.disconnect(other)
	time.Sleep(2 * electionTimeoutMax)
	c.checkNoLeader()

	// Majority restored: a leader appears again.
	c.connect(other)
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestElectionTimeDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("slow measurement")
	}

	c := newCluster(t, 5)
	trials := 50
	var durations []time.Duration

	for trial := 0; trial < trials; trial++ {
		oldLeader := c.checkOneLeader()
		oldTerm, _ := c.net.node(oldLeader).GetState()

		c.disconnect(oldLeader)
		start := time.Now()
		for !c.newLeaderAbove(oldTerm) {
			time.Sleep(time.Millisecond)
			if time.Since(start) > 5*time.Second {
				t.Fatalf("trial %d: no new leader after 5s", trial)
			}
		}
		durations = append(durations, time.Since(start))
		c.connect(oldLeader)
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("5 nodes, %d leader failures", trials)
	t.Logf("failover time: min %v  p50 %v  p90 %v  max %v",
		durations[0], durations[len(durations)/2], durations[len(durations)*9/10], durations[len(durations)-1])
}
