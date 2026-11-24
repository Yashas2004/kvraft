package main

import (
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"github.com/Yashas2004/kvraft/raft"
)

func main() {
	id := flag.Int("id", 0, "this node's index in -peers")
	peers := flag.String("peers", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "comma-separated addresses of all nodes")
	flag.Parse()

	addrs := strings.Split(*peers, ",")
	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("-id must be between 0 and %d", len(addrs)-1)
	}

	ln, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Fatal(err)
	}
	rf := raft.NewRaft(*id, len(addrs), raft.NewTCPTransport(addrs), nil, nil)
	if err := raft.Serve(rf, ln); err != nil {
		log.Fatal(err)
	}
	log.Printf("node %d listening on %s", *id, addrs[*id])

	// Print every role or term change.
	lastTerm := -1
	lastIsLeader := false
	for {
		time.Sleep(100 * time.Millisecond)
		term, isLeader := rf.GetState()
		if term != lastTerm || isLeader != lastIsLeader {
			role := "follower/candidate"
			if isLeader {
				role = "LEADER"
			}
			log.Printf("term %d: %s", term, role)
			lastTerm = term
			lastIsLeader = isLeader
		}
	}
}
