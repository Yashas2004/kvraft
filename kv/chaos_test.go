package kv

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Yashas2004/kvraft/raft"
	"github.com/Yashas2004/kvraft/storage"
)

// ---------- Porcupine model: a map from key to string ----------

const (
	modelGet = iota
	modelPut
	modelAppend
)

type kvInput struct {
	op    int
	key   string
	value string
}

type kvOutput struct {
	value string
}

var kvModel = porcupine.Model{
	// Linearizability is local (Herlihy & Wing): check each key separately.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(kvInput).key
			byKey[key] = append(byKey[key], op)
		}
		var partitions [][]porcupine.Operation
		for _, ops := range byKey {
			partitions = append(partitions, ops)
		}
		return partitions
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(kvInput)
		current := state.(string)
		switch in.op {
		case modelGet:
			return output.(kvOutput).value == current, current
		case modelPut:
			return true, in.value
		default: // modelAppend
			return true, current + in.value
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		switch in.op {
		case modelGet:
			return fmt.Sprintf("get(%s) -> %q", in.key, output.(kvOutput).value)
		case modelPut:
			return fmt.Sprintf("put(%s, %q)", in.key, in.value)
		default:
			return fmt.Sprintf("append(%s, %q)", in.key, in.value)
		}
	},
}

// ---------- chaos cluster: 5 KV servers on an in-memory network ----------

type chaosCluster struct {
	t          *testing.T
	n          int
	net        *raft.MemNetwork
	maxRaftLog int

	mu         sync.Mutex
	servers    []*Server
	dbs        []*storage.DB
	persisters []*raft.Persister
	raftDirs   []string
	dbDirs     []string
	alive      []bool
}

func newChaosCluster(t *testing.T, n, maxRaftLog int) *chaosCluster {
	c := &chaosCluster{
		t:          t,
		n:          n,
		net:        raft.NewMemNetwork(n),
		maxRaftLog: maxRaftLog,
		servers:    make([]*Server, n),
		dbs:        make([]*storage.DB, n),
		persisters: make([]*raft.Persister, n),
		raftDirs:   make([]string, n),
		dbDirs:     make([]string, n),
		alive:      make([]bool, n),
	}
	for i := 0; i < n; i++ {
		c.raftDirs[i] = t.TempDir()
		c.dbDirs[i] = t.TempDir()
		c.start(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			if c.isAlive(i) {
				c.crash(i) // closes files before TempDir cleanup
			}
		}
	})
	return c
}

// start boots node i from its own directories, like a real process restart.
// Called from the chaos loop, so it panics instead of t.Fatal on setup errors.
func (c *chaosCluster) start(i int) {
	persister, err := raft.OpenPersister(c.raftDirs[i])
	if err != nil {
		panic(err)
	}
	db, err := storage.OpenDB(c.dbDirs[i], false) // same DB directory as before the crash
	if err != nil {
		panic(err)
	}
	applyCh := make(chan raft.ApplyMsg)
	rf := raft.NewRaft(i, c.n, c.net.Transport(i), applyCh, persister)
	server := NewServer(rf, db, applyCh, c.maxRaftLog)
	c.net.SetNode(i, rf)

	c.mu.Lock()
	c.servers[i] = server
	c.dbs[i] = db
	c.persisters[i] = persister
	c.alive[i] = true
	c.mu.Unlock()
}

func (c *chaosCluster) crash(i int) {
	c.net.SetConnected(i, false)
	c.mu.Lock()
	server, db, persister := c.servers[i], c.dbs[i], c.persisters[i]
	c.alive[i] = false
	c.mu.Unlock()

	server.Kill()
	db.Close()
	persister.Close()
}

func (c *chaosCluster) restart(i int) {
	c.start(i)
	c.net.SetConnected(i, true)
}

func (c *chaosCluster) server(i int) *Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

func (c *chaosCluster) isAlive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive[i]
}

func (c *chaosCluster) downCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	down := 0
	for _, a := range c.alive {
		if !a {
			down++
		}
	}
	return down
}

// checkReplicasConverge: once every replica has applied the same log index,
// their storage engines must hold exactly the same data.
func (c *chaosCluster) checkReplicasConverge() {
	for attempt := 0; attempt < 100; attempt++ {
		applied := make([]int, c.n)
		states := make([]map[string]string, c.n)
		for i := 0; i < c.n; i++ {
			s := c.server(i)
			s.applyMu.Lock() // consistent view: no apply in progress
			entries, err := s.db.ScanAll()
			applied[i] = s.lastApplied
			s.applyMu.Unlock()
			if err != nil {
				c.t.Fatal(err)
			}
			states[i] = make(map[string]string)
			for _, e := range entries {
				states[i][string(e.Key)] = string(e.Value)
			}
		}

		allCaughtUp := true
		for i := 1; i < c.n; i++ {
			if applied[i] != applied[0] {
				allCaughtUp = false
			}
		}
		if !allCaughtUp {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for i := 1; i < c.n; i++ {
			for key, want := range states[0] {
				if got := states[i][key]; got != want {
					c.t.Fatalf("replicas diverged at index %d, key %q:\n node 0 (%d bytes): %.120s\n node %d (%d bytes): %.120s",
						applied[0], key, len(want), want, i, len(got), got)
				}
			}
			if len(states[i]) != len(states[0]) {
				c.t.Fatalf("replicas diverged at index %d: node 0 has %d keys, node %d has %d",
					applied[0], len(states[0]), i, len(states[i]))
			}
		}
		c.t.Logf("all %d replicas converged at index %d", c.n, applied[0])
		return
	}
	c.t.Fatalf("replicas never caught up to the same index")
}

// ---------- history recorder ----------

type recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

func (r *recorder) now() int64 {
	return time.Since(r.start).Nanoseconds()
}

func (r *recorder) add(op porcupine.Operation) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// ---------- the chaos run ----------

type chaosConfig struct {
	maxRaftLog int
	unreliable bool
	partitions bool
	crashes    bool
	duration   time.Duration
}

func runChaos(t *testing.T, cfg chaosConfig) {
	if testing.Short() {
		t.Skip("chaos test")
	}
	c := newChaosCluster(t, 5, cfg.maxRaftLog)
	c.net.SetUnreliable(cfg.unreliable)
	rec := &recorder{start: time.Now()}
	// "x" only ever receives appends: no Put can mask a replayed/duplicated append.
	// More keys = shorter per-key histories, so the checker can finish.
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "x"}

	var stop atomic.Bool
	var wg sync.WaitGroup
	for cl := 0; cl < 5; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			ck := NewClerkFunc(c.n, c.server)
			for i := 0; !stop.Load(); i++ {
				// Think time keeps histories small enough to check exhaustively.
				time.Sleep(time.Duration(5+rand.Intn(10)) * time.Millisecond)
				key := keys[rand.Intn(len(keys))]
				input := kvInput{key: key}
				var output kvOutput
				var err error

				call := rec.now()
				r := rand.Intn(4)
				if key == "x" && r >= 2 && r < 3 {
					r = 0 // never Put to the append-only key
				}
				switch {
				case r < 2:
					input.op = modelAppend
					input.value = fmt.Sprintf("c%d.%d;", cl, i)
					err = ck.Append([]byte(key), []byte(input.value))
				case r == 2:
					input.op = modelPut
					input.value = fmt.Sprintf("P%d.%d;", cl, i)
					err = ck.Put([]byte(key), []byte(input.value))
				default:
					input.op = modelGet
					var value []byte
					value, _, err = ck.Get([]byte(key))
					output.value = string(value)
				}
				ret := rec.now()

				if err != nil {
					if input.op == modelGet {
						continue // a failed read changed nothing
					}
					ret = math.MaxInt64 // a failed write may or may not have happened
				}
				rec.add(porcupine.Operation{ClientId: cl, Input: input, Call: call, Output: output, Return: ret})
			}
		}(cl)
	}

	events := 0
	deadline := time.Now().Add(cfg.duration)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)

		if cfg.partitions {
			if rand.Intn(3) == 0 {
				c.net.Heal()
			} else {
				groups := make([]int, c.n)
				minority := 1 + rand.Intn(2) // 1 or 2 nodes cut off
				for _, i := range rand.Perm(c.n)[:minority] {
					groups[i] = 1
				}
				c.net.Partition(groups)
			}
			events++
		}
		if cfg.crashes && rand.Intn(2) == 0 {
			victim := rand.Intn(c.n)
			if c.isAlive(victim) {
				if c.downCount() < 2 {
					c.crash(victim)
					events++
				}
			} else {
				c.restart(victim)
				events++
			}
		}
	}

	// Calm down: heal, restart everyone, let in-flight operations finish.
	c.net.Heal()
	c.net.SetUnreliable(false)
	for i := 0; i < c.n; i++ {
		if !c.isAlive(i) {
			c.restart(i)
		}
	}
	time.Sleep(time.Second)
	stop.Store(true)
	wg.Wait()

	// One final write gives every replica a common index to catch up to.
	if err := NewClerkFunc(c.n, c.server).Put([]byte("end"), []byte("1")); err != nil {
		t.Fatalf("final write failed: %v", err)
	}
	c.checkReplicasConverge()

	rec.mu.Lock()
	ops := rec.ops
	rec.mu.Unlock()

	checkStart := time.Now()
	result := porcupine.CheckOperationsTimeout(kvModel, ops, 60*time.Second)
	t.Logf("checker took %v", time.Since(checkStart))
	switch result {
	case porcupine.Illegal:
		t.Fatalf("NOT linearizable: %d ops, %d chaos events", len(ops), events)
	case porcupine.Unknown:
		t.Fatalf("checker timed out on %d ops: inconclusive is not a pass", len(ops))
	default:
		t.Logf("linearizable: %d ops, %d chaos events", len(ops), events)
	}
}

func TestChaosUnreliable(t *testing.T) {
	runChaos(t, chaosConfig{unreliable: true, duration: 5 * time.Second})
}

func TestChaosPartitions(t *testing.T) {
	runChaos(t, chaosConfig{unreliable: true, partitions: true, duration: 5 * time.Second})
}

func TestChaosCrashes(t *testing.T) {
	runChaos(t, chaosConfig{crashes: true, duration: 5 * time.Second})
}

func TestChaosCrashesSnapshots(t *testing.T) {
	runChaos(t, chaosConfig{crashes: true, maxRaftLog: 30, duration: 5 * time.Second})
}

func TestChaosEverything(t *testing.T) {
	runChaos(t, chaosConfig{
		unreliable: true, partitions: true, crashes: true,
		maxRaftLog: 30, duration: 8 * time.Second,
	})
}
