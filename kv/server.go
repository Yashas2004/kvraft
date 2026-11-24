package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
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

	requestTimeout = time.Second
)

// Command format: op (1B) | keyLen (4B) | key | value
func encodeCommand(op byte, key, value []byte) []byte {
	buf := make([]byte, 5+len(key)+len(value))
	buf[0] = op
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(key)))
	copy(buf[5:], key)
	copy(buf[5+len(key):], value)
	return buf
}

func decodeCommand(cmd []byte) (byte, []byte, []byte) {
	keyLen := int(binary.LittleEndian.Uint32(cmd[1:5]))
	key := cmd[5 : 5+keyLen]
	value := cmd[5+keyLen:]
	return cmd[0], key, value
}

// Snapshot format: count (4B) | { keyLen (4B) | valLen (4B) | key | value } ...
func encodeSnapshot(entries []storage.Entry) []byte {
	size := 4
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
	return buf
}

func decodeSnapshot(data []byte) ([]storage.Entry, error) {
	if len(data) < 4 {
		return nil, errors.New("kv: snapshot too short")
	}
	count := int(binary.LittleEndian.Uint32(data[0:4]))
	pos := 4
	entries := make([]storage.Entry, 0, count)
	for i := 0; i < count; i++ {
		if pos+8 > len(data) {
			return nil, errors.New("kv: corrupt snapshot")
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		valLen := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		pos += 8
		if pos+keyLen+valLen > len(data) {
			return nil, errors.New("kv: corrupt snapshot")
		}
		key := data[pos : pos+keyLen]
		pos += keyLen
		value := data[pos : pos+valLen]
		pos += valLen
		entries = append(entries, storage.Entry{Key: key, Value: value})
	}
	return entries, nil
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
	maxRaftLog int                 // snapshot when Raft keeps more entries than this (0 = never)

	// applyMu is held for every apply (including snapshots). Kill takes it too,
	// so shutdown waits for an in-progress apply and nothing runs after it.
	applyMu     sync.Mutex
	dead        bool
	lastApplied int

	// Snapshot timing (protected by applyMu)
	snapshots int
	scanTime  time.Duration
	saveTime  time.Duration
}

// NewServer applies every committed command from applyCh to db.
// applyCh must be the same channel passed to raft.NewRaft.
func NewServer(rf *raft.Raft, db *storage.DB, applyCh <-chan raft.ApplyMsg, maxRaftLog int) *Server {
	s := &Server{rf: rf, db: db, waiters: make(map[int]chan result), maxRaftLog: maxRaftLog}
	go s.applyLoop(applyCh)
	return s
}

func (s *Server) IsLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

// Kill stops Raft and waits for any in-progress apply to finish.
// After it returns, the storage engine can be closed safely.
func (s *Server) Kill() {
	s.rf.Kill()
	s.applyMu.Lock()
	s.dead = true
	s.applyMu.Unlock()
}

// SnapshotStats returns how many snapshots were taken and the total time
// spent scanning the storage engine vs. saving the snapshot in Raft.
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
		// After Kill we keep draining applyCh so Raft's applier never blocks forever.
	}
}

// apply runs with applyMu held.
func (s *Server) apply(msg raft.ApplyMsg) {
	if msg.SnapshotValid {
		// From InstallSnapshot or a restart: replace the whole database.
		if msg.SnapshotIndex <= s.lastApplied {
			return
		}
		entries, err := decodeSnapshot(msg.Snapshot)
		if err != nil {
			panic(fmt.Sprintf("kv: %v", err))
		}
		if err := s.db.RestoreFrom(entries); err != nil {
			panic(fmt.Sprintf("kv: restore snapshot: %v", err))
		}
		s.lastApplied = msg.SnapshotIndex
		return
	}
	if msg.Index <= s.lastApplied {
		return // already covered by a snapshot we restored
	}

	op, key, value := decodeCommand(msg.Command)
	r := result{term: msg.Term}
	switch op {
	case opPut:
		r.err = s.db.Put(key, value)
	case opDelete:
		r.err = s.db.Delete(key)
	case opGet:
		r.value, r.found, r.err = s.db.Get(key)
	}
	s.lastApplied = msg.Index

	s.mu.Lock()
	if ch, ok := s.waiters[msg.Index]; ok {
		ch <- r // buffered, never blocks
		delete(s.waiters, msg.Index)
	}
	s.mu.Unlock()

	// Checkpoint: the DB now reflects exactly entries 1..msg.Index.
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
	s.rf.Snapshot(index, encodeSnapshot(entries))

	s.snapshots++
	s.scanTime += scanned.Sub(start)
	s.saveTime += time.Since(scanned)
}

func (s *Server) submit(op byte, key, value []byte) (result, error) {
	cmd := encodeCommand(op, key, value)
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
		return r, r.err
	case <-time.After(requestTimeout):
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return result{}, ErrTimeout
	}
}

func (s *Server) Put(key, value []byte) error {
	_, err := s.submit(opPut, key, value)
	return err
}

func (s *Server) Delete(key []byte) error {
	_, err := s.submit(opDelete, key, nil)
	return err
}

// Get goes through the log so it is linearizable: a stale leader can't answer.
func (s *Server) Get(key []byte) ([]byte, bool, error) {
	r, err := s.submit(opGet, key, nil)
	if err != nil {
		return nil, false, err
	}
	return r.value, r.found, nil
}
