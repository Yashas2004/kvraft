package kv

import (
	"math/rand"
	"time"
)

// Clerk is a client. It retries each request (same seq) until some server
// answers, trying servers in turn to find the leader. Not safe for concurrent
// use: one outstanding request per client is what makes per-client seq dedup work.
type Clerk struct {
	servers  []*Server
	clientID int64
	seq      int64
	leader   int // last server that answered; try it first
}

func NewClerk(servers []*Server) *Clerk {
	return &Clerk{
		servers:  servers,
		clientID: rand.Int63n(1<<62) + 1, // random, non-zero
	}
}

func (ck *Clerk) call(op byte, key, value []byte) ([]byte, bool, error) {
	ck.seq++ // same seq for every retry of THIS request
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, err := ck.servers[ck.leader].submit(op, ck.clientID, ck.seq, key, value)
		if err == nil {
			return r.value, r.found, nil
		}
		// Wrong leader, lost leadership, or no reply: try the next server.
		ck.leader = (ck.leader + 1) % len(ck.servers)
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
