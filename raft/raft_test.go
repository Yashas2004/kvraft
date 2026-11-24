package raft

import (
	"fmt"
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
	t        *testing.T
	net      *network
	n        int
	mu       sync.Mutex
	logs     [][]string // logs[i] = commands applied by node i, in order
	finished bool
}

func newCluster(t *testing.T, n int) *cluster {
	net := &network{nodes: make([]*Raft, n), connected: make([]bool, n)}
	for i := 0; i < n; i++ {
		net.connected[i] = true
	}
	c := &cluster{t: t, net: net, n: n, logs: make([][]string, n)}

	for i := 0; i < n; i++ {
		applyCh := make(chan ApplyMsg)
		go c.applyLoop(i, applyCh)
		rf := NewRaft(i, n, &localTransport{net: net, from: i}, applyCh)
		net.mu.Lock()
		net.nodes[i] = rf
		net.mu.Unlock()
	}

	t.Cleanup(func() {
		c.mu.Lock()
		c.finished = true // stop reporting from background goroutines
		c.mu.Unlock()
		for _, rf := range net.nodes {
			rf.Kill()
		}
	})
	return c
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

func TestRejoinDoesNotDisrupt(t *testing.T) {
	c := newCluster(t, 3)

	for round := 0; round < 5; round++ {
		oldLeader := c.checkOneLeader()
		c.disconnect(oldLeader)
		c.checkOneLeader()
		time.Sleep(50 * time.Millisecond)
		termBefore := c.checkTermsAgree() // agreement among the 2 connected nodes

		c.connect(oldLeader)
		time.Sleep(2 * electionTimeoutMax)

		termAfter := c.checkTermsAgree()
		if termAfter != termBefore {
			t.Fatalf("round %d: rejoining node forced an election (term %d -> %d)", round, termBefore, termAfter)
		}
	}
}

// ---------- replication helpers ----------

func (c *cluster) applyLoop(i int, applyCh chan ApplyMsg) {
	for msg := range applyCh {
		c.recordApply(i, msg)
	}
}

func (c *cluster) recordApply(i int, msg ApplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}

	expected := len(c.logs[i]) + 1
	if msg.Index != expected {
		c.t.Errorf("node %d applied index %d, expected %d (out of order)", i, msg.Index, expected)
		return
	}
	command := string(msg.Command)
	for j := 0; j < c.n; j++ {
		if j != i && len(c.logs[j]) >= msg.Index && c.logs[j][msg.Index-1] != command {
			c.t.Errorf("index %d: node %d applied %q but node %d applied %q",
				msg.Index, i, command, j, c.logs[j][msg.Index-1])
		}
	}
	c.logs[i] = append(c.logs[i], command)
}

// nCommitted returns how many nodes applied an entry at index, and what it was.
func (c *cluster) nCommitted(index int) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	command := ""
	for i := 0; i < c.n; i++ {
		if len(c.logs[i]) >= index {
			count++
			command = c.logs[i][index-1]
		}
	}
	return count, command
}

// one submits a command to whichever node is leader and waits until at least
// expectedServers have applied it. Retries if leadership changes. Returns the index.
func (c *cluster) one(command string, expectedServers int) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if !c.isConnected(i) {
				continue
			}
			index, _, isLeader := c.net.node(i).Start([]byte(command))
			if !isLeader {
				continue
			}

			waitStart := time.Now()
			for time.Since(waitStart) < 2*time.Second {
				count, applied := c.nCommitted(index)
				if count >= expectedServers && applied == command {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			break // didn't commit in time; leadership may have changed, retry
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("command %q never committed on %d servers", command, expectedServers)
	return -1
}

// ---------- replication tests ----------

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3)
	for i := 1; i <= 3; i++ {
		index := c.one(fmt.Sprintf("cmd-%d", i), 3)
		if index != i {
			t.Fatalf("expected index %d, got %d", i, index)
		}
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3)

	leader := c.checkOneLeader()
	follower := (leader + 1) % 3
	c.disconnect(follower)

	// 2 of 3 is still a majority: progress continues.
	c.one("b", 2)
	c.one("c", 2)

	// Follower returns and must catch up on everything it missed.
	c.connect(follower)
	last := c.one("d", 3)
	for index := 1; index <= last; index++ {
		if count, _ := c.nCommitted(index); count != 3 {
			t.Fatalf("index %d applied on %d nodes, expected 3", index, count)
		}
	}
}

func TestLeaderFailure(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3)

	leader := c.checkOneLeader()
	c.disconnect(leader)
	c.one("b", 2) // remaining two elect a new leader and keep committing

	c.connect(leader)
	c.one("c", 3) // old leader rejoins and catches up
}

// The isolated old leader accepts entries that can never commit. After it
// rejoins, those entries must be overwritten by the majority's history.
func TestConflictingLogsAreRepaired(t *testing.T) {
	c := newCluster(t, 3)
	c.one("start", 3)

	oldLeader := c.checkOneLeader()
	c.disconnect(oldLeader)
	for i := 1; i <= 3; i++ {
		c.net.node(oldLeader).Start([]byte(fmt.Sprintf("lost-%d", i))) // will never commit
	}

	for i := 1; i <= 3; i++ {
		c.one(fmt.Sprintf("won-%d", i), 2)
	}

	c.connect(oldLeader)
	c.one("final", 3)

	for index := 2; index <= 4; index++ {
		count, command := c.nCommitted(index)
		if count != 3 || command != fmt.Sprintf("won-%d", index-1) {
			t.Fatalf("index %d: expected won-%d on 3 nodes, got %q on %d", index, index-1, command, count)
		}
	}
}
