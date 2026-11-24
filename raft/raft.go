package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Leader"
	}
}

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command []byte
}

// ApplyMsg is sent to the state machine for every committed entry, in index order.
type ApplyMsg struct {
	Index   int
	Command []byte
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int // on failure: where the leader should retry from
	ConflictTerm  int // on failure: term of the conflicting entry, or -1 if our log is too short
}

type Transport interface {
	RequestVote(peer int, args RequestVoteArgs, reply *RequestVoteReply) bool
	AppendEntries(peer int, args AppendEntriesArgs, reply *AppendEntriesReply) bool
}

type Raft struct {
	mu        sync.Mutex
	id        int
	peerCount int
	transport Transport
	dead      atomic.Bool
	applyCh   chan<- ApplyMsg // may be nil (election-only tests)
	applyCond *sync.Cond      // signals the applier when commitIndex moves

	// Persistent state (Figure 2). Saved to disk in Week 6.
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a dummy (term 0), so real entries start at index 1

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders (reset on every election win)
	nextIndex  []int // next log index to send to each peer
	matchIndex []int // highest index known to be replicated on each peer

	state             State
	lastHeard         time.Time
	electionTimeout   time.Duration
	lastHeartbeatSent time.Time
}

func NewRaft(id, peerCount int, transport Transport, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{
		id:        id,
		peerCount: peerCount,
		transport: transport,
		applyCh:   applyCh,
		votedFor:  -1,
		log:       []LogEntry{{Term: 0}},
		state:     Follower,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()
	go rf.ticker()
	go rf.applier()
	return rf
}

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// ---------- helpers: caller holds rf.mu ----------

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	index := len(rf.log) - 1
	return index, rf.log[index].Term
}

func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, rf.peerCount)
	rf.matchIndex = make([]int, rf.peerCount)
	for i := 0; i < rf.peerCount; i++ {
		rf.nextIndex[i] = len(rf.log) // optimistic: assume followers match us
		rf.matchIndex[i] = 0          // pessimistic: nothing confirmed yet
	}
	rf.matchIndex[rf.id] = len(rf.log) - 1
	rf.lastHeartbeatSent = time.Time{} // heartbeat on the next tick
}

// advanceCommitIndex finds the highest index stored on a majority
// whose entry is from the current term (§5.4.2), and commits up to it.
func (rf *Raft) advanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue // never commit old-term entries by counting (Figure 8)
		}
		count := 0
		for peer := 0; peer < rf.peerCount; peer++ {
			if rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count > rf.peerCount/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// ---------- public API ----------

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Start proposes a command. Returns the index it will occupy if committed,
// the current term, and whether this node is the leader. No guarantee it commits:
// the leader could fail first.
func (rf *Raft) Start(command []byte) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1
	term := rf.currentTerm
	rf.matchIndex[rf.id] = index
	rf.advanceCommitIndex() // only matters for a 1-node cluster
	rf.mu.Unlock()

	rf.broadcastAppendEntries() // replicate now instead of waiting for the next heartbeat
	return index, term, true
}

func (rf *Raft) Kill() {
	rf.dead.Store(true)
	rf.mu.Lock()
	rf.applyCond.Broadcast() // wake the applier so it can exit
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// ---------- background goroutines ----------

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)

		rf.mu.Lock()
		shouldElect := false
		shouldHeartbeat := false
		if rf.state == Leader {
			rf.lastHeard = time.Now() // so a stepped-down leader waits a full timeout
			if time.Since(rf.lastHeartbeatSent) >= heartbeatInterval {
				rf.lastHeartbeatSent = time.Now()
				shouldHeartbeat = true
			}
		} else if time.Since(rf.lastHeard) >= rf.electionTimeout {
			shouldElect = true
		}
		rf.mu.Unlock()

		if shouldElect {
			rf.startElection()
		}
		if shouldHeartbeat {
			rf.broadcastAppendEntries()
		}
	}
}

// applier is the consumer: it waits for commitIndex to move, then delivers
// entries in order. It never holds mu while sending on applyCh.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// Mesa semantics: always re-check the condition after waking.
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			return
		}

		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied].Command})
		}

		rf.mu.Unlock()
		if rf.applyCh != nil {
			for _, msg := range msgs {
				rf.applyCh <- msg
			}
		}
		rf.mu.Lock()
	}
}

// ---------- elections ----------

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.id
	rf.resetElectionTimer()
	term := rf.currentTerm
	lastIndex, lastTerm := rf.lastLogIndexAndTerm()
	if rf.peerCount == 1 {
		rf.becomeLeader()
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	votes := 1
	for peer := 0; peer < rf.peerCount; peer++ {
		if peer == rf.id {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{Term: term, CandidateID: rf.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
			var reply RequestVoteReply
			if !rf.transport.RequestVote(peer, args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > rf.peerCount/2 {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

func (rf *Raft) HandleRequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	// Election restriction (§5.4.1): candidate's log must be at least as up-to-date as ours.
	myLastIndex, myLastTerm := rf.lastLogIndexAndTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
}

// ---------- replication ----------

func (rf *Raft) broadcastAppendEntries() {
	for peer := 0; peer < rf.peerCount; peer++ {
		if peer != rf.id {
			go rf.replicateTo(peer)
		}
	}
}

func (rf *Raft) replicateTo(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[peer]
	prevIndex := next - 1
	entries := make([]LogEntry, len(rf.log)-next)
	copy(entries, rf.log[next:]) // copy: the log may change after we unlock
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.log[prevIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.transport.AppendEntries(peer, args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return // stale reply from an older term
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] { // replies can arrive out of order
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitIndex()
		return
	}

	// Rejected. Ignore if nextIndex already moved since we sent this.
	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex // follower's log is too short
	} else {
		lastIndexOfTerm := -1
		for i := len(rf.log) - 1; i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastIndexOfTerm = i
				break
			}
		}
		if lastIndexOfTerm != -1 {
			rf.nextIndex[peer] = lastIndexOfTerm + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex // skip the follower's whole conflicting term
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
	go rf.replicateTo(peer) // retry right away from the earlier point
}

func (rf *Raft) HandleAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Consistency check: do we have the entry right before the new ones?
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index > 1 && rf.log[index-1].Term == reply.ConflictTerm {
			index-- // find the first entry of the conflicting term
		}
		reply.ConflictIndex = index
		return
	}

	// Append new entries. Truncate ONLY at a real conflict, so a delayed,
	// older AppendEntries can never erase entries we already accepted.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term == entry.Term {
				continue // already have this exact entry
			}
			rf.log = rf.log[:index] // conflict: drop it and everything after
		}
		rf.log = append(rf.log, entry)
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Signal()
		}
	}
	reply.Success = true
}
