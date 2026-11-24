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

// ApplyMsg carries either one committed entry, or (SnapshotValid) a snapshot
// that replaces the state machine's state up to SnapshotIndex. Delivered in order.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type Transport interface {
	RequestVote(peer int, args RequestVoteArgs, reply *RequestVoteReply) bool
	AppendEntries(peer int, args AppendEntriesArgs, reply *AppendEntriesReply) bool
	InstallSnapshot(peer int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool
}

type Raft struct {
	mu        sync.Mutex // lock order: rf.mu before persister.mu, everywhere
	id        int
	peerCount int
	transport Transport
	persister *Persister
	dead      atomic.Bool
	applyCh   chan<- ApplyMsg
	applyCond *sync.Cond
	syncCond  *sync.Cond

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a placeholder: its Term is the snapshot's last term

	// Snapshot / compaction. Global index i lives at log[i - lastIncludedIndex].
	lastIncludedIndex int
	snapshot          []byte
	snapshotPending   bool // snapshot must be delivered to the service before more entries

	persistedIndex int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

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
		st, err := persister.LoadState()
		if err != nil {
			panic(fmt.Sprintf("raft %d: cannot load persisted state: %v", id, err))
		}
		rf.currentTerm = st.Term
		rf.votedFor = st.VotedFor
		rf.log = st.Log
		rf.lastIncludedIndex = st.SnapshotIndex
		rf.snapshot = st.Snapshot
		if st.SnapshotIndex > 0 {
			rf.commitIndex = st.SnapshotIndex // a snapshot only ever covers committed entries
			rf.snapshotPending = true         // hand it to the service before replaying entries
		}
	}
	rf.persistedIndex = rf.lastIndex()
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.syncCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()
	go rf.ticker()
	go rf.applier()
	if persister != nil {
		go rf.leaderSyncer()
	}
	return rf
}

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// ---------- index helpers: caller holds rf.mu ----------

func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

// entriesFrom returns the log from global index onward (shares memory: copy before unlocking).
func (rf *Raft) entriesFrom(index int) []LogEntry {
	return rf.log[index-rf.lastIncludedIndex:]
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	last := rf.lastIndex()
	return last, rf.termAt(last)
}

// ---------- persistence helpers: caller holds rf.mu ----------

func (rf *Raft) persistTermVote() {
	if rf.persister == nil {
		return
	}
	if err := rf.persister.SaveTermVote(rf.currentTerm, rf.votedFor); err != nil {
		panic(fmt.Sprintf("raft %d: persist term/vote: %v", rf.id, err))
	}
}

func (rf *Raft) persistLog(fromIndex int, truncate bool) {
	if rf.persister != nil {
		if err := rf.persister.AppendLog(fromIndex, rf.entriesFrom(fromIndex), truncate); err != nil {
			panic(fmt.Sprintf("raft %d: persist log: %v", rf.id, err))
		}
	}
	rf.persistedIndex = rf.lastIndex()
}

func (rf *Raft) persistSnapshot() {
	if rf.persister != nil {
		err := rf.persister.SaveSnapshot(rf.lastIncludedIndex, rf.log[0].Term, rf.snapshot, rf.log[1:])
		if err != nil {
			panic(fmt.Sprintf("raft %d: persist snapshot: %v", rf.id, err))
		}
	}
	rf.persistedIndex = rf.lastIndex() // the rewrite included every entry we hold
}

// ---------- state helpers: caller holds rf.mu ----------

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	rf.electionTimeout = randomElectionTimeout()
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
		rf.nextIndex[i] = rf.lastIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.id] = rf.persistedIndex
	rf.lastHeartbeatSent = time.Time{}
	rf.syncCond.Signal()
}

func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue // Figure 8
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

// LogLength is the number of entries kept since the last snapshot.
func (rf *Raft) LogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1
}

func (rf *Raft) Start(command []byte) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := rf.lastIndex()
	term := rf.currentTerm
	if rf.persister == nil {
		rf.persistedIndex = index
		rf.matchIndex[rf.id] = index
		rf.advanceCommitIndex()
	} else {
		rf.syncCond.Signal()
	}
	rf.mu.Unlock()

	rf.broadcastAppendEntries()
	return index, term, true
}

// Snapshot is called by the service: "my state includes everything up to index;
// here it is." Raft discards those entries and persists the snapshot.
func (rf *Raft) Snapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastApplied {
		return // already covered, or not applied yet
	}
	newLog := make([]LogEntry, 0, rf.lastIndex()-index+1)
	newLog = append(newLog, LogEntry{Term: rf.termAt(index)})
	newLog = append(newLog, rf.entriesFrom(index+1)...)

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = data
	rf.persistSnapshot()
}

func (rf *Raft) Kill() {
	rf.dead.Store(true)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.syncCond.Broadcast()
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
		for !rf.snapshotPending && rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			return
		}

		var msgs []ApplyMsg
		if rf.snapshotPending {
			// A snapshot always goes first: it replaces state up to lastIncludedIndex.
			msgs = append(msgs, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.log[0].Term,
			})
			rf.lastApplied = rf.lastIncludedIndex
			rf.snapshotPending = false
		} else {
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				entry := rf.log[rf.lastApplied-rf.lastIncludedIndex]
				msgs = append(msgs, ApplyMsg{Index: rf.lastApplied, Term: entry.Term, Command: entry.Command})
			}
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

func (rf *Raft) leaderSyncer() {
	for {
		rf.mu.Lock()
		for !rf.killed() && !(rf.state == Leader && rf.persistedIndex < rf.lastIndex()) {
			rf.syncCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		from := rf.persistedIndex + 1
		to := rf.lastIndex()
		source := rf.entriesFrom(from)
		entries := make([]LogEntry, len(source))
		copy(entries, source)
		term := rf.currentTerm

		rf.persister.mu.Lock() // order: rf.mu -> persister.mu
		rf.mu.Unlock()
		err := rf.persister.appendLocked(from, entries, false)
		rf.persister.mu.Unlock()
		if err != nil {
			panic(fmt.Sprintf("raft %d: leader sync: %v", rf.id, err))
		}

		rf.mu.Lock()
		if rf.state == Leader && rf.currentTerm == term {
			if to > rf.persistedIndex {
				rf.persistedIndex = to
			}
			rf.matchIndex[rf.id] = rf.persistedIndex
			rf.advanceCommitIndex()
		}
		rf.mu.Unlock()
	}
}

// ---------- elections ----------

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.id
	rf.persistTermVote()
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
		rf.persistTermVote()
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

// sendAppendEntriesOnce sends one AppendEntries (or InstallSnapshot if the
// follower needs entries we've already compacted). Returns true to resend at once.
func (rf *Raft) sendAppendEntriesOnce(peer int) bool {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return false
	}
	next := rf.nextIndex[peer]

	if next <= rf.lastIncludedIndex {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.id,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.snapshot,
		}
		term := rf.currentTerm
		rf.mu.Unlock()
		return rf.sendSnapshot(peer, args, term)
	}

	prevIndex := next - 1
	source := rf.entriesFrom(next)
	entries := make([]LogEntry, len(source))
	copy(entries, source)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.termAt(prevIndex),
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
		for i := rf.lastIndex(); i > rf.lastIncludedIndex; i-- {
			if rf.termAt(i) == reply.ConflictTerm {
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

func (rf *Raft) sendSnapshot(peer int, args InstallSnapshotArgs, term int) bool {
	var reply InstallSnapshotReply
	if !rf.transport.InstallSnapshot(peer, args, &reply) {
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
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	rf.advanceCommitIndex()
	return true // follow up right away with the entries after the snapshot
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

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// Our snapshot already covers PrevLogIndex: ask the leader to start after it.
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex > rf.lastIndex() {
		reply.ConflictIndex = rf.lastIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		index := args.PrevLogIndex
		for index > rf.lastIncludedIndex+1 && rf.termAt(index-1) == reply.ConflictTerm {
			index--
		}
		reply.ConflictIndex = index
		return
	}

	truncateAt := -1
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= rf.lastIndex() {
			if rf.termAt(index) == entry.Term {
				continue
			}
			rf.log = rf.log[:index-rf.lastIncludedIndex]
			truncateAt = index
		}
		rf.log = append(rf.log, entry)
	}

	needTruncate := truncateAt != -1 && truncateAt <= rf.persistedIndex
	if needTruncate {
		rf.persistLog(truncateAt, true)
	} else if rf.persistedIndex < rf.lastIndex() {
		rf.persistLog(rf.persistedIndex+1, false)
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

func (rf *Raft) HandleInstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return // we already have (and will apply) everything it covers
	}

	// Keep entries after the snapshot only if our log agrees at its last index.
	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if args.LastIncludedIndex < rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog = append(newLog, rf.entriesFrom(args.LastIncludedIndex+1)...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshotPending = true
	rf.persistSnapshot() // durable before we reply
	rf.applyCond.Signal()
}
