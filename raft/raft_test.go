package raft

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func (lt *localTransport) InstallSnapshot(peer int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if !lt.net.reachable(lt.from, peer) {
		return false
	}
	time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
	lt.net.node(peer).HandleInstallSnapshot(args, reply)
	return lt.net.reachable(lt.from, peer)
}

// ---------- cluster helpers ----------

type cluster struct {
	t             *testing.T
	net           *network
	n             int
	mu            sync.Mutex
	logs          [][]string // logs[i] = commands applied by node i's current incarnation
	generation    []int      // bumped on every restart; old incarnations' applies are ignored
	persisters    []*Persister
	dirs          []string
	snapshotEvery int // > 0: test state machine snapshots every N applied entries
	finished      bool
}

func newCluster(t *testing.T, n int) *cluster {
	return newClusterOpts(t, n, 0)
}

func newClusterOpts(t *testing.T, n, snapshotEvery int) *cluster {
	net := &network{nodes: make([]*Raft, n), connected: make([]bool, n)}
	for i := 0; i < n; i++ {
		net.connected[i] = true
	}
	c := &cluster{
		t:             t,
		net:           net,
		n:             n,
		logs:          make([][]string, n),
		generation:    make([]int, n),
		persisters:    make([]*Persister, n),
		dirs:          make([]string, n),
		snapshotEvery: snapshotEvery,
	}
	for i := 0; i < n; i++ {
		c.dirs[i] = t.TempDir()
		c.start(i)
	}

	t.Cleanup(func() {
		c.mu.Lock()
		c.finished = true // stop reporting from background goroutines
		c.mu.Unlock()
		for i := 0; i < n; i++ {
			c.net.node(i).Kill()
			c.persisters[i].Close() // before TempDir cleanup: Windows can't delete open files
		}
	})
	return c
}

// start boots node i from whatever its persister has on disk.
func (c *cluster) start(i int) {
	persister, err := OpenPersister(c.dirs[i])
	if err != nil {
		c.t.Fatal(err)
	}

	c.mu.Lock()
	c.generation[i]++
	gen := c.generation[i]
	c.logs[i] = nil // a restarted node rebuilds from its snapshot + log
	c.persisters[i] = persister
	c.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	rf := NewRaft(i, c.n, &localTransport{net: c.net, from: i}, applyCh, persister)
	go c.applyLoop(i, gen, rf, applyCh) // applier blocks on the channel until this starts

	c.net.mu.Lock()
	c.net.nodes[i] = rf
	c.net.mu.Unlock()
}

// crash: node stops instantly and keeps only what reached its disk.
func (c *cluster) crash(i int) {
	c.disconnect(i)
	c.net.node(i).Kill()
	c.mu.Lock()
	persister := c.persisters[i]
	c.mu.Unlock()
	persister.Close() // any write after this point is lost, like a power cut
}

func (c *cluster) restart(i int) {
	c.start(i)
	c.connect(i)
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

// checkOneLeader waits until exactly one connected node believes it is leader
// (stale leaders from older terms must have stepped down). Two leaders in the
// SAME term is a safety violation and fails immediately.
func (c *cluster) checkOneLeader() int {
	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(50 * time.Millisecond)

		leadersByTerm := make(map[int][]int)
		totalLeaders := 0
		for i := 0; i < c.n; i++ {
			if !c.isConnected(i) {
				continue
			}
			term, isLeader := c.net.node(i).GetState()
			if isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], i)
				totalLeaders++
			}
		}

		for term, ids := range leadersByTerm {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
		}
		if totalLeaders == 1 {
			for _, ids := range leadersByTerm {
				return ids[0]
			}
		}
		// zero leaders (election in progress) or a stale leader hasn't stepped down yet: wait
	}
	c.t.Fatalf("expected exactly one leader")
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

// ---------- election tests ----------

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

func encodeCommands(commands []string) []byte {
	return []byte(strings.Join(commands, "\x00"))
}

func decodeCommands(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	return strings.Split(string(data), "\x00")
}

func (c *cluster) applyLoop(i, gen int, rf *Raft, applyCh chan ApplyMsg) {
	for msg := range applyCh {
		snapIndex, snapData := c.recordApply(i, gen, msg)
		if snapIndex > 0 {
			rf.Snapshot(snapIndex, snapData) // outside c.mu
		}
	}
}

// recordApply checks and records one ApplyMsg. The test "state machine" is the
// list of applied commands. Returns (index, data) when it wants a snapshot taken.
func (c *cluster) recordApply(i, gen int, msg ApplyMsg) (int, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || gen != c.generation[i] {
		return 0, nil // test over, or message from a crashed incarnation
	}

	if msg.SnapshotValid {
		commands := decodeCommands(msg.Snapshot)
		if len(commands) != msg.SnapshotIndex {
			c.t.Errorf("node %d: snapshot at %d holds %d commands", i, msg.SnapshotIndex, len(commands))
			return 0, nil
		}
		for index := 1; index <= len(commands); index++ {
			for j := 0; j < c.n; j++ {
				if j != i && len(c.logs[j]) >= index && c.logs[j][index-1] != commands[index-1] {
					c.t.Errorf("snapshot index %d: node %d has %q but node %d applied %q",
						index, i, commands[index-1], j, c.logs[j][index-1])
				}
			}
		}
		c.logs[i] = commands
		return 0, nil
	}

	expected := len(c.logs[i]) + 1
	if msg.Index != expected {
		c.t.Errorf("node %d applied index %d, expected %d (out of order)", i, msg.Index, expected)
		return 0, nil
	}
	command := string(msg.Command)
	for j := 0; j < c.n; j++ {
		if j != i && len(c.logs[j]) >= msg.Index && c.logs[j][msg.Index-1] != command {
			c.t.Errorf("index %d: node %d applied %q but node %d applied %q",
				msg.Index, i, command, j, c.logs[j][msg.Index-1])
		}
	}
	c.logs[i] = append(c.logs[i], command)

	if c.snapshotEvery > 0 && msg.Index%c.snapshotEvery == 0 {
		return msg.Index, encodeCommands(c.logs[i])
	}
	return 0, nil
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

// ---------- persistence tests ----------

func TestPersisterFile(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	p.SaveTermVote(5, 2)
	p.AppendLog(1, []LogEntry{
		{Term: 1, Command: []byte("a")},
		{Term: 1, Command: []byte("b")},
		{Term: 2, Command: []byte("c")},
	}, false)
	p.AppendLog(2, []LogEntry{{Term: 3, Command: []byte("x")}}, true) // replace b, c with x
	p.Close()

	// Simulate a crash mid-write: garbage at the end of the log file.
	f, err := os.OpenFile(filepath.Join(dir, "raft-log"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{9, 9, 9, 9, 9})
	f.Close()

	p2, err := OpenPersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	st, err := p2.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if st.Term != 5 || st.VotedFor != 2 {
		t.Fatalf("term/vote: got %d/%d, expected 5/2", st.Term, st.VotedFor)
	}
	if len(st.Log) != 3 || string(st.Log[1].Command) != "a" || string(st.Log[2].Command) != "x" || st.Log[2].Term != 3 {
		t.Fatalf("log wrong after truncate + torn tail: %+v", st.Log)
	}
}

func TestRestartAll(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3)

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}

	index := c.one("b", 3)
	if index != 2 {
		t.Fatalf("expected b at index 2 (log survived restart), got %d", index)
	}
	if count, command := c.nCommitted(1); count != 3 || command != "a" {
		t.Fatalf("index 1 after restart: %q on %d nodes", command, count)
	}
}

func TestCrashLeaderAndRestart(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3)

	leader := c.checkOneLeader()
	c.crash(leader)
	c.one("b", 2)

	c.restart(leader)
	last := c.one("c", 3)
	for index := 1; index <= last; index++ {
		if count, _ := c.nCommitted(index); count != 3 {
			t.Fatalf("index %d applied on %d nodes, expected 3", index, count)
		}
	}
}

func TestPersistChurn(t *testing.T)  { runChurn(t, 0) }
func TestSnapshotChurn(t *testing.T) { runChurn(t, 5) }

// Random crashes and restarts while commands are proposed. The apply checker
// fails the test if any two nodes ever apply different commands at one index.
func runChurn(t *testing.T, snapshotEvery int) {
	c := newClusterOpts(t, 5, snapshotEvery)
	c.one("init", 5)
	alive := []bool{true, true, true, true, true}

	for iter := 0; iter < 30; iter++ {
		for i := 0; i < 5; i++ {
			if alive[i] {
				c.net.node(i).Start([]byte(fmt.Sprintf("churn-%d-%d", iter, i)))
			}
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

		down := 0
		for i := 0; i < 5; i++ {
			if !alive[i] {
				down++
			}
		}
		victim := rand.Intn(5)
		if alive[victim] && down < 2 {
			c.crash(victim) // never more than 2 of 5 down: a majority stays up
			alive[victim] = false
		} else if !alive[victim] {
			c.restart(victim)
			alive[victim] = true
		}
	}

	for i := 0; i < 5; i++ {
		if !alive[i] {
			c.restart(i)
		}
	}
	c.one("final", 5)
}

// ---------- snapshot tests ----------

// A follower misses so much that the leader has compacted the entries it
// needs: it can only catch up through InstallSnapshot.
func TestSnapshotLaggingFollower(t *testing.T) {
	c := newClusterOpts(t, 3, 10)
	c.one("first", 3)

	leader := c.checkOneLeader()
	follower := (leader + 1) % 3
	c.disconnect(follower)

	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("cmd-%d", i), 2)
	}
	leader = c.checkOneLeader()
	if length := c.net.node(leader).LogLength(); length >= 20 {
		t.Fatalf("leader log not compacted: %d entries kept", length)
	}

	c.connect(follower)
	last := c.one("final", 3)
	for index := 1; index <= last; index++ {
		if count, _ := c.nCommitted(index); count != 3 {
			t.Fatalf("index %d on %d nodes after snapshot catch-up", index, count)
		}
	}
}

// Restart from snapshot + log tail; the log stays short instead of holding all history.
func TestSnapshotRestart(t *testing.T) {
	c := newClusterOpts(t, 3, 10)
	for i := 1; i <= 35; i++ {
		c.one(fmt.Sprintf("cmd-%d", i), 3)
	}

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}

	index := c.one("after", 3)
	if index != 36 {
		t.Fatalf("expected index 36 after restart, got %d", index)
	}
	for i := 1; i <= 36; i++ {
		if count, _ := c.nCommitted(i); count != 3 {
			t.Fatalf("index %d on %d nodes after restart", i, count)
		}
	}
	for i := 0; i < 3; i++ {
		if length := c.net.node(i).LogLength(); length >= 20 {
			t.Fatalf("node %d kept %d log entries; snapshot should bound it", i, length)
		}
	}
}
