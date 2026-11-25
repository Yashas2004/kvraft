package kv

import "net/rpc"

// Request is one client operation over the network.
type Request struct {
	Op       byte
	ClientID int64
	Seq      int64
	Key      []byte
	Value    []byte
}

// Response carries errors as text ("kv: not the leader", ...) so they are never
// confused with network failures, which arrive as RPC errors instead.
type Response struct {
	Value []byte
	Found bool
	Err   string
}

type KVService struct {
	s *Server
}

func (k *KVService) Do(req Request, resp *Response) error {
	r, err := k.s.submit(req.Op, req.ClientID, req.Seq, req.Key, req.Value)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.Value = r.value
	resp.Found = r.found
	return nil
}

// RegisterRPC adds the KV service to an RPC server (alongside raft.RegisterRPC).
func RegisterRPC(server *rpc.Server, s *Server) error {
	return server.RegisterName("KV", &KVService{s: s})
}