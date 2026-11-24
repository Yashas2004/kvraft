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

type RequestVoteArgs struct {
	Term        int
	CandidateID int
	// Week 5 adds LastLogIndex and LastLogTerm.
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// Week 5 adds PrevLogIndex, PrevLogTerm, Entries, LeaderCommit.
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Transport sends RPCs to other nodes. Returns false if the peer was unreachable.
// Tests use an in-memory network; Part B adds a real TCP one.
type Transport interface {
	RequestVote(peer int, args RequestVoteArgs, reply *RequestVoteReply) bool
	AppendEntries(peer int, args AppendEntriesArgs, reply *AppendEntriesReply) bool
}

type Raft struct {
	mu        sync.Mutex
	id        int
	peerCount int // total nodes, including this one; ids are 0..peerCount-1
	transport Transport
	dead      atomic.Bool

	// Persistent state (Figure 2). Must be saved to disk before replying: Week 6.
	currentTerm int
	votedFor    int // -1 means "haven't voted this term"

	// Volatile state
	state             State
	lastHeard         time.Time // last time we heard from a leader or granted a vote
	electionTimeout   time.Duration
	lastHeartbeatSent time.Time
}

func NewRaft(id, peerCount int, transport Transport) *Raft {
	rf := &Raft{
		id:        id,
		peerCount: peerCount,
		transport: transport,
		votedFor:  -1,
		state:     Follower,
	}
	rf.resetElectionTimer()
	go rf.ticker()
	return rf
}

func randomElectionTimeout() time.Duration {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// The helpers below assume the caller already holds rf.mu.

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1 // new term, fresh vote
	}
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.lastHeartbeatSent = time.Time{} // zero time: ticker sends a heartbeat on its next tick
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) Kill() {
	rf.dead.Store(true)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// ticker drives everything time-based. It decides under the lock,
// then acts outside it, because we never hold mu across an RPC.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)

		rf.mu.Lock()
		shouldElect := false
		shouldHeartbeat := false
		if rf.state == Leader {
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
			rf.sendHeartbeats()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.id
	rf.resetElectionTimer() // if this election splits, the next one starts after a new random timeout
	term := rf.currentTerm
	if rf.peerCount == 1 {
		rf.becomeLeader() // a one-node cluster is its own majority
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	votes := 1 // our own vote; protected by rf.mu
	for peer := 0; peer < rf.peerCount; peer++ {
		if peer == rf.id {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{Term: term, CandidateID: rf.id}
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
			// Ignore replies from an election we're no longer running.
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

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	for peer := 0; peer < rf.peerCount; peer++ {
		if peer == rf.id {
			continue
		}
		go func(peer int) {
			args := AppendEntriesArgs{Term: term, LeaderID: rf.id}
			var reply AppendEntriesReply
			if !rf.transport.AppendEntries(peer, args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term) // someone has a newer term: step down
			}
		}(peer)
	}
}

// HandleRequestVote is the receiver side of RequestVote (Figure 2).
func (rf *Raft) HandleRequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return // stale candidate
	}
	// Week 5 adds: only vote if the candidate's log is at least as up-to-date as ours (§5.4.1).
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.resetElectionTimer() // we just backed a candidate; give them time to win
	}
}

// HandleAppendEntries is the receiver side of AppendEntries. For now: heartbeats only.
func (rf *Raft) HandleAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return // message from an old leader
	}

	// A valid leader exists for this term: follow it (this also stops a candidate).
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	reply.Success = true
}
