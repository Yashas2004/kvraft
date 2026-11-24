package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Yashas2004/kvraft/raft"
	"github.com/Yashas2004/kvraft/storage"
)

var (
	ErrWrongLeader    = errors.New("kv: not the leader")
	ErrLostLeadership = errors.New("kv: leadership lost before commit")
	ErrTimeout        = errors.New("kv: request timed out")
)

const (
	opPut    byte = 1
	opDelete byte = 2
	opGet    byte = 3
	opAppend byte = 4

	requestTimeout = time.Second
)

// Command format: op (1B) | clientID (8B) | seq (8B) | keyLen (4B) | key | value
// clientID 0 means "no deduplication" (used by benchmarks).
func encodeCommand(op byte, clientID, seq int64, key, value []byte) []byte {
	buf := make([]byte, 21+len(key)+len(value))
	buf[0] = op
	binary.LittleEndian.PutUint64(buf[1:9], uint64(clientID))
	binary.LittleEndian.PutUint64(buf[9:17], uint64(seq))
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(key)))
	copy(buf[21:], key)
	copy(buf[21+len(key):], value)
	return buf
}

func decodeCommand(cmd []byte) (byte, int64, int64, []byte, []byte) {
	clientID := int64(binary.LittleEndian.Uint64(cmd[1:9]))
	seq := int64(binary.LittleEndian.Uint64(cmd[9:17]))
	keyLen := int(binary.LittleEndian.Uint32(cmd[17:21]))
	key := cmd[21 : 21+keyLen]
	value := cmd[21+keyLen:]
	return cmd[0], clientID, seq, key, value
}

// Snapshot format:
//
//	entryCount (4B) | { keyLen (4B) | valLen (4B) | key | value } ...
//	clientCount (4B) | { clientID (8B) | lastSeq (8B) } ...
func encodeSnapshot(entries []storage.Entry, lastSeq map[int64]int64) []byte {
	size := 4 + 4 + 16*len(lastSeq)
	for _, e := range entries {
		size += 8 + len(e.Key) + len(e.Value)
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(entries)))
	pos := 4
	for _, e := range entries {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(e.Key)))
		binary.LittleEndian.PutUint32(buf[pos+4:pos+8], uint32(len(e.Value)))
		pos += 8
		copy(buf[pos:], e.Key)
		pos += len(e.Key)
		copy(buf[pos:], e.Value)
		pos += len(e.Value)
	}
	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(len(lastSeq)))
	pos += 4
	for clientID, seq := range lastSeq {
		binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(clientID))
		binary.LittleEndian.PutUint64(buf[pos+8:pos+16], uint64(seq))
		pos += 16
	}
	return buf
}

func decodeSnapshot(data []byte) ([]storage.Entry, map[int64]int64, error) {
	corrupt := errors.New("kv: corrupt snapshot")
	if len(data) < 4 {
		return nil, nil, corrupt
	}
	count := int(binary.LittleEndian.Uint32(data[0:4]))
	pos := 4
	entries := make([]storage.Entry, 0, count)
	for i := 0; i < count; i++ {
		if pos+8 > len(data) {
			return nil, nil, corrupt
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		valLen := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		pos += 8
		if pos+keyLen+valLen > len(data) {
			return nil, nil, corrupt
		}
		key := data[pos : pos+keyLen]
		pos += keyLen
		value := data[pos : pos+valLen]
		pos += valLen
		entries = append(entries, storage.Entry{Key: key, Value: value})
	}

	if pos+4 > len(data) {
		return nil, nil, corrupt
	}
	clients := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	lastSeq := make(map[int64]int64, clients)
	for i := 0; i < clients; i++ {
		if pos+16 > len(data) {
			return nil, nil, corrupt
		}
		clientID := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		seq := int64(binary.LittleEndian.Uint64(data[pos+8 : pos+16]))
		lastSeq[clientID] = seq
		pos += 16
	}
	return entries, lastSeq, nil
}

type result struct {
	term  int
	value []byte
	found bool
	err   error
}

type Server struct {
	rf         *raft.Raft
	db         *storage.DB
	mu         sync.Mutex          // protects waiters; lock order: s.mu, then rf.mu
	waiters    map[int]chan result // log index -> client waiting for it
	maxRaftLog int

	applyMu     sync.Mutex // held for every apply; Kill takes it too
	dead        bool
	lastApplied int
	lastSeq     map[int64]int64 // clientID -> highest seq applied (dedup table, in snapshots)

	snapshots int
	scanTime  time.Duration
	saveTime  time.Duration

	// Test hook: probability of "losing" a reply AFTER the command was applied.
	// Set before clients start; never changed while requests are in flight.
	dropReplyProb float64
}

func NewServer(rf *raft.Raft, db *storage.DB, applyCh <-chan raft.ApplyMsg, maxRaftLog int) *Server {
	s := &Server{
		rf:         rf,
		db:         db,
		waiters:    make(map[int]chan result),
		maxRaftLog: maxRaftLog,
		lastSeq:    make(map[int64]int64),
	}
	go s.applyLoop(applyCh)
	return s
}

func (s *Server) IsLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

func (s *Server) Kill() {
	s.rf.Kill()
	s.applyMu.Lock()
	s.dead = true
	s.applyMu.Unlock()
}

func (s *Server) SnapshotStats() (int, time.Duration, time.Duration) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	return s.snapshots, s.scanTime, s.saveTime
}

func (s *Server) applyLoop(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		s.applyMu.Lock()
		if !s.dead {
			s.apply(msg)
		}
		s.applyMu.Unlock()
	}
}

// apply runs with applyMu held.
func (s *Server) apply(msg raft.ApplyMsg) {
	if msg.SnapshotValid {
		if msg.SnapshotIndex <= s.lastApplied {
			return
		}
		entries, lastSeq, err := decodeSnapshot(msg.Snapshot)
		if err != nil {
			panic(fmt.Sprintf("kv: %v", err))
		}
		if err := s.db.RestoreFrom(entries); err != nil {
			panic(fmt.Sprintf("kv: restore snapshot: %v", err))
		}
		s.lastSeq = lastSeq
		s.lastApplied = msg.SnapshotIndex
		return
	}
	if msg.Index <= s.lastApplied {
		return
	}

	op, clientID, seq, key, value := decodeCommand(msg.Command)
	r := result{term: msg.Term}

	// Every replica sees the same log in the same order, so every replica
	// makes the same duplicate decision. Gets never change state: no dedup needed.
	duplicate := clientID != 0 && op != opGet && seq <= s.lastSeq[clientID]
	if !duplicate {
		switch op {
		case opPut:
			r.err = s.db.Put(key, value)
		case opAppend:
			old, _, err := s.db.Get(key)
			if err != nil {
				r.err = err
			} else {
				newValue := make([]byte, 0, len(old)+len(value))
				newValue = append(newValue, old...)
				newValue = append(newValue, value...)
				r.err = s.db.Put(key, newValue)
			}
		case opDelete:
			r.err = s.db.Delete(key)
		case opGet:
			r.value, r.found, r.err = s.db.Get(key)
		}
		if clientID != 0 && op != opGet {
			s.lastSeq[clientID] = seq
		}
	}
	s.lastApplied = msg.Index

	s.mu.Lock()
	if ch, ok := s.waiters[msg.Index]; ok {
		ch <- r
		delete(s.waiters, msg.Index)
	}
	s.mu.Unlock()

	if s.maxRaftLog > 0 && s.rf.SnapshotReady(msg.Index, s.maxRaftLog) {
		s.takeSnapshot(msg.Index)
	}
}

func (s *Server) takeSnapshot(index int) {
	start := time.Now()
	entries, err := s.db.ScanAll()
	if err != nil {
		panic(fmt.Sprintf("kv: scan for snapshot: %v", err))
	}
	scanned := time.Now()
	s.rf.Snapshot(index, encodeSnapshot(entries, s.lastSeq))

	s.snapshots++
	s.scanTime += scanned.Sub(start)
	s.saveTime += time.Since(scanned)
}

func (s *Server) submit(op byte, clientID, seq int64, key, value []byte) (result, error) {
	cmd := encodeCommand(op, clientID, seq, key, value)
	ch := make(chan result, 1)

	s.mu.Lock()
	index, term, isLeader := s.rf.Start(cmd)
	if !isLeader {
		s.mu.Unlock()
		return result{}, ErrWrongLeader
	}
	s.waiters[index] = ch
	s.mu.Unlock()

	select {
	case r := <-ch:
		if r.term != term {
			return result{}, ErrLostLeadership
		}
		if s.dropReplyProb > 0 && rand.Float64() < s.dropReplyProb {
			return result{}, ErrTimeout // applied, but the client never hears back
		}
		return r, r.err
	case <-time.After(requestTimeout):
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return result{}, ErrTimeout
	}
}

// Put, Delete and Get without deduplication (benchmarks). Real clients use Clerk.
func (s *Server) Put(key, value []byte) error {
	_, err := s.submit(opPut, 0, 0, key, value)
	return err
}

func (s *Server) Delete(key []byte) error {
	_, err := s.submit(opDelete, 0, 0, key, nil)
	return err
}

func (s *Server) Get(key []byte) ([]byte, bool, error) {
	r, err := s.submit(opGet, 0, 0, key, nil)
	if err != nil {
		return nil, false, err
	}
	return r.value, r.found, nil
}
