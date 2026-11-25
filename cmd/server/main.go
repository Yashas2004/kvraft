package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/Yashas2004/kvraft/kv"
	"github.com/Yashas2004/kvraft/raft"
	"github.com/Yashas2004/kvraft/storage"
)

func main() {
	id := flag.Int("id", 0, "this node's index in -peers")
	peers := flag.String("peers", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "comma-separated addresses of all nodes")
	dataRoot := flag.String("data", "data", "root directory for node data")
	maxRaftLog := flag.Int("snapshot", 10000, "snapshot when the Raft log exceeds this many entries (0 = never)")
	flag.Parse()

	addrs := strings.Split(*peers, ",")
	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("-id must be between 0 and %d", len(addrs)-1)
	}

	nodeDir := filepath.Join(*dataRoot, fmt.Sprintf("node%d", *id))
	persister, err := raft.OpenPersister(filepath.Join(nodeDir, "raft"))
	if err != nil {
		log.Fatal(err)
	}
	db, err := storage.OpenDB(filepath.Join(nodeDir, "db"), false)
	if err != nil {
		log.Fatal(err)
	}

	applyCh := make(chan raft.ApplyMsg)
	rf := raft.NewRaft(*id, len(addrs), raft.NewTCPTransport(addrs), applyCh, persister)
	server := kv.NewServer(rf, db, applyCh, *maxRaftLog)

	// One port serves both Raft peers and KV clients.
	rpcServer := rpc.NewServer()
	if err := raft.RegisterRPC(rpcServer, rf); err != nil {
		log.Fatal(err)
	}
	if err := kv.RegisterRPC(rpcServer, server); err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Fatal(err)
	}
	go rpcServer.Accept(ln)
	log.Printf("node %d listening on %s, data in %s", *id, addrs[*id], nodeDir)

	// Ctrl+C (SIGINT) can be caught: shut down cleanly.
	// A hard kill (SIGKILL / End task) cannot; persistence makes that safe too.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		log.Printf("node %d shutting down", *id)
		ln.Close()
		server.Kill()
		db.Close()
		persister.Close()
		os.Exit(0)
	}()

	lastTerm := -1
	lastIsLeader := false
	for {
		time.Sleep(100 * time.Millisecond)
		term, isLeader := rf.GetState()
		if term != lastTerm || isLeader != lastIsLeader {
			role := rf.Role()
			log.Printf("term %d: %s", term, role)
			lastTerm = term
			lastIsLeader = isLeader
		}
	}
}
