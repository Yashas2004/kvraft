package kv

import (
	"encoding/binary"
	"errors"
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

type result struct {
	term  int
	value []byte
	found bool
	err   error
}

type Server struct {
	rf      *raft.Raft
	db      *storage.DB
	mu      sync.Mutex          // lock order: s.mu, then (inside Start) rf.mu
	waiters map[int]chan result // log index -> client waiting for it
}

// NewServer applies every committed command from applyCh to db.
// applyCh must be the same channel passed to raft.NewRaft.
func NewServer(rf *raft.Raft, db *storage.DB, applyCh <-chan raft.ApplyMsg) *Server {
	s := &Server{rf: rf, db: db, waiters: make(map[int]chan result)}
	go s.applyLoop(applyCh)
	return s
}

func (s *Server) IsLeader() bool {
	_, isLeader := s.rf.GetState()
	return isLeader
}

func (s *Server) applyLoop(applyCh <-chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.SnapshotValid {
			continue // B2: restore the storage engine from the snapshot
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

		s.mu.Lock()
		if ch, ok := s.waiters[msg.Index]; ok {
			ch <- r // buffered, never blocks
			delete(s.waiters, msg.Index)
		}
		s.mu.Unlock()
	}
}

func (s *Server) submit(op byte, key, value []byte) (result, error) {
	cmd := encodeCommand(op, key, value)
	ch := make(chan result, 1)

	// Hold s.mu across Start + registration so the applier can't
	// deliver this index before we're listening for it.
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
			return result{}, ErrLostLeadership // a different entry won this index
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
