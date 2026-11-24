package kv

import (
	"math/rand"
	"time"
)

// Clerk is a client. It retries each request (same seq) until some server
// answers, trying servers in turn to find the leader. Not safe for concurrent
// use: one outstanding request per client is what makes per-client seq dedup work.
type Clerk struct {
	server   func(i int) *Server // current instance of server i (it may be restarted)
	n        int
	clientID int64
	seq      int64
	leader   int
}

func NewClerk(servers []*Server) *Clerk {
	return NewClerkFunc(len(servers), func(i int) *Server { return servers[i] })
}

func NewClerkFunc(n int, server func(i int) *Server) *Clerk {
	return &Clerk{
		server:   server,
		n:        n,
		clientID: rand.Int63n(1<<62) + 1, // random, non-zero
	}
}

func (ck *Clerk) call(op byte, key, value []byte) ([]byte, bool, error) {
	ck.seq++ // same seq for every retry of THIS request
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, err := ck.server(ck.leader).submit(op, ck.clientID, ck.seq, key, value)
		if err == nil {
			return r.value, r.found, nil
		}
		ck.leader = (ck.leader + 1) % ck.n
		time.Sleep(5 * time.Millisecond)
	}
	return nil, false, ErrTimeout
}

func (ck *Clerk) Put(key, value []byte) error {
	_, _, err := ck.call(opPut, key, value)
	return err
}

func (ck *Clerk) Append(key, value []byte) error {
	_, _, err := ck.call(opAppend, key, value)
	return err
}

func (ck *Clerk) Delete(key []byte) error {
	_, _, err := ck.call(opDelete, key, nil)
	return err
}

func (ck *Clerk) Get(key []byte) ([]byte, bool, error) {
	return ck.call(opGet, key, nil)
}
