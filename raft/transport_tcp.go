package raft

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const rpcTimeout = 100 * time.Millisecond

var errStopped = errors.New("raft: node stopped")

// RaftService exposes a node's handlers over net/rpc.
type RaftService struct {
	rf *Raft
}

func (s *RaftService) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if s.rf.killed() {
		return errStopped
	}
	s.rf.HandleRequestVote(args, reply)
	return nil
}

func (s *RaftService) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if s.rf.killed() {
		return errStopped
	}
	s.rf.HandleAppendEntries(args, reply)
	return nil
}

// Serve accepts RPC connections on ln in the background.
// Each connection gets its own goroutine; Accept returns once ln is closed.
func Serve(rf *Raft, ln net.Listener) error {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &RaftService{rf: rf}); err != nil {
		return err
	}
	go server.Accept(ln)
	return nil
}

// TCPTransport keeps one persistent connection per peer and redials on failure.
type TCPTransport struct {
	mu      sync.Mutex
	addrs   []string
	clients map[int]*rpc.Client
}

var _ Transport = (*TCPTransport)(nil)

func NewTCPTransport(addrs []string) *TCPTransport {
	return &TCPTransport{addrs: addrs, clients: make(map[int]*rpc.Client)}
}

func (t *TCPTransport) getClient(peer int) (*rpc.Client, error) {
	t.mu.Lock()
	if client, ok := t.clients[peer]; ok {
		t.mu.Unlock()
		return client, nil
	}
	t.mu.Unlock()

	// Dial outside the lock: never hold a mutex across network I/O.
	conn, err := net.DialTimeout("tcp", t.addrs[peer], rpcTimeout)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.clients[peer]; ok {
		client.Close() // another goroutine connected first; use theirs
		return existing, nil
	}
	t.clients[peer] = client
	return client, nil
}

func (t *TCPTransport) dropClient(peer int, client *rpc.Client) {
	t.mu.Lock()
	if t.clients[peer] == client {
		delete(t.clients, peer)
	}
	t.mu.Unlock()
	client.Close()
}

func (t *TCPTransport) call(peer int, method string, args any, reply any) bool {
	client, err := t.getClient(peer)
	if err != nil {
		return false
	}

	call := client.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			// A ServerError means the peer answered with an error (e.g. stopped);
			// the connection itself is fine. Anything else: connection is broken.
			if _, isServerError := call.Error.(rpc.ServerError); !isServerError {
				t.dropClient(peer, client)
			}
			return false
		}
		return true
	case <-time.After(rpcTimeout):
		t.dropClient(peer, client) // probably dead; redial next time
		return false
	}
}

func (t *TCPTransport) RequestVote(peer int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return t.call(peer, "Raft.RequestVote", args, reply)
}

func (t *TCPTransport) AppendEntries(peer int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return t.call(peer, "Raft.AppendEntries", args, reply)
}
