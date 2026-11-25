package kv

import (
	"math/rand"
	"net"
	"net/rpc"
	"time"
)

const netRequestTimeout = 2 * time.Second // > server's 1 s requestTimeout

// NetClerk is Clerk over TCP: same client ID + seq retry protocol, so
// retries across servers and lost replies are still applied exactly once.
// Not safe for concurrent use (one outstanding request per client).
type NetClerk struct {
	addrs    []string
	conns    []*rpc.Client
	clientID int64
	seq      int64
	leader   int
}

func NewNetClerk(addrs []string) *NetClerk {
	return &NetClerk{
		addrs:    addrs,
		conns:    make([]*rpc.Client, len(addrs)),
		clientID: rand.Int63n(1<<62) + 1,
	}
}

func (ck *NetClerk) conn(i int) (*rpc.Client, error) {
	if ck.conns[i] == nil {
		c, err := net.DialTimeout("tcp", ck.addrs[i], 500*time.Millisecond)
		if err != nil {
			return nil, err
		}
		ck.conns[i] = rpc.NewClient(c)
	}
	return ck.conns[i], nil
}

func (ck *NetClerk) drop(i int) {
	if ck.conns[i] != nil {
		ck.conns[i].Close()
		ck.conns[i] = nil
	}
}

func (ck *NetClerk) call(op byte, key, value []byte) ([]byte, bool, error) {
	ck.seq++ // same seq for every retry of this request
	req := Request{Op: op, ClientID: ck.clientID, Seq: ck.seq, Key: key, Value: value}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		i := ck.leader
		client, err := ck.conn(i)
		if err == nil {
			var resp Response
			call := client.Go("KV.Do", req, &resp, make(chan *rpc.Call, 1))
			select {
			case <-call.Done:
				if call.Error == nil && resp.Err == "" {
					return resp.Value, resp.Found, nil
				}
				if call.Error != nil {
					ck.drop(i) // network-level failure: redial next time
				}
			case <-time.After(netRequestTimeout):
				ck.drop(i)
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.addrs) // try the next server
		time.Sleep(10 * time.Millisecond)
	}
	return nil, false, ErrTimeout
}

func (ck *NetClerk) Put(key, value []byte) error {
	_, _, err := ck.call(opPut, key, value)
	return err
}

func (ck *NetClerk) Append(key, value []byte) error {
	_, _, err := ck.call(opAppend, key, value)
	return err
}

func (ck *NetClerk) Delete(key []byte) error {
	_, _, err := ck.call(opDelete, key, nil)
	return err
}

func (ck *NetClerk) Get(key []byte) ([]byte, bool, error) {
	return ck.call(opGet, key, nil)
}

func (ck *NetClerk) Close() {
	for i := range ck.conns {
		ck.drop(i)
	}
}
