package raft

import (
	"fmt"
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
	Term    int
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
	ConflictIndex int
	ConflictTerm  int
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
	persister *Persister // nil = memory only (some tests, benchmarks)
	dead      atomic.Bool
	applyCh   chan<- ApplyMsg
	applyCond *sync.Cond

	// Persistent state (Figure 2): on disk before replying to any RPC.
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a dummy (term 0)

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Per-peer replication control: at most one AppendEntries in flight.
	inFlight []bool
	pending  []bool

	state             State
	lastHeard         time.Time
	electionTimeout   time.Duration
	lastHeartbeatSent time.Time
}

func NewRaft(id, peerCount int, transport Transport, applyCh chan<- ApplyMsg, persister *Persister) *Raft {
	rf := &Raft{
		id:        id,
		peerCount: peerCount,
		transport: transport,
		persister: persister,
		applyCh:   applyCh,
		votedFor:  -1,
		log:       []LogEntry{{Term: 0}},
		inFlight:  make([]bool, peerCount),
		pending:   make([]bool, peerCount),
		state:     Follower,
	}
	if persister != nil {
		term, votedFor, log, err := persister.LoadState()
		if err != nil {
			panic(fmt.Sprintf("raft %d: cannot load persisted state: %v", id, err))
		}
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
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

// A node that can't write to disk can't safely keep participating: stop hard.
func (rf *Raft) persistTermVote() {
	if rf.persister == nil {
		return
	}
	if err := rf.persister.SaveTermVote(rf.currentTerm, rf.votedFor); err != nil {
		panic(fmt.Sprintf("raft %d: persist term/vote: %v", rf.id, err))
	}
}

func (rf *Raft) persistLog(fromIndex int, truncate bool) {
	if rf.persister == nil {
		return
	}
	if err := rf.persister.AppendLog(fromIndex, rf.log[fromIndex:], truncate); err != nil {
		panic(fmt.Sprintf("raft %d: persist log: %v", rf.id, err))
	}
}

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
		rf.persistTermVote()
	}
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, rf.peerCount)
	rf.matchIndex = make([]int, rf.peerCount)
	for i := 0; i < rf.peerCount; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.id] = len(rf.log) - 1
	rf.lastHeartbeatSent = time.Time{}
}

func (rf *Raft) advanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue // Figure 8: only count replicas of current-term entries
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
	rf.persistLog(index, false) // leader's own copy is durable before it counts toward a majority
	rf.matchIndex[rf.id] = index
	rf.advanceCommitIndex()
	rf.mu.Unlock()

	rf.broadcastAppendEntries()
	return index, term, true
}

func (rf *Raft) Kill() {
	rf.dead.Store(true)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
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
			rf.lastHeard = time.Now()
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

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			return
		}

		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{Index: rf.lastApplied, Term: rf.log[rf.lastApplied].Term, Command: rf.log[rf.lastApplied].Command})
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
	rf.persistTermVote() // our own vote must survive a crash too
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

	myLastIndex, myLastTerm := rf.lastLogIndexAndTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persistTermVote() // vote is on disk before the reply leaves
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
	if rf.inFlight[peer] {
		rf.pending[peer] = true
		rf.mu.Unlock()
		return
	}
	rf.inFlight[peer] = true
	rf.mu.Unlock()

	for !rf.killed() {
		retry := rf.sendAppendEntriesOnce(peer)

		rf.mu.Lock()
		if (!retry && !rf.pending[peer]) || rf.state != Leader {
			rf.inFlight[peer] = false
			rf.pending[peer] = false
			rf.mu.Unlock()
			return
		}
		rf.pending[peer] = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesOnce(peer int) bool {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}
	next := rf.nextIndex[peer]
	prevIndex := next - 1
	entries := make([]LogEntry, len(rf.log)-next)
	copy(entries, rf.log[next:])
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
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return false
	}
	if rf.state != Leader || rf.currentTerm != term {
		return false
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitIndex()
		return false
	}

	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return false
	}
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
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
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
	return true
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

	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index > 1 && rf.log[index-1].Term == reply.ConflictTerm {
			index--
		}
		reply.ConflictIndex = index
		return
	}

	// Append new entries; truncate only at a real conflict.
	firstNew := -1
	truncated := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:index]
			truncated = true
		}
		if firstNew == -1 {
			firstNew = index
		}
		rf.log = append(rf.log, entry)
	}
	if firstNew != -1 {
		rf.persistLog(firstNew, truncated) // one fsync for the whole batch, before replying
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
