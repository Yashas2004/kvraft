package storage

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestEngineRestart(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenDB(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Delete([]byte("a"))
	db.Put([]byte("c"), []byte("3"))
	db.Close()

	db2, err := OpenDB(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if _, found, _ := db2.Get([]byte("a")); found {
		t.Fatalf("a was deleted but came back")
	}
	value, found, _ := db2.Get([]byte("b"))
	if !found || string(value) != "2" {
		t.Fatalf("b: expected 2, got %q", value)
	}
	value, found, _ = db2.Get([]byte("c"))
	if !found || string(value) != "3" {
		t.Fatalf("c: expected 3, got %q", value)
	}
}

func TestEngineCrashRecovery(t *testing.T) {
	// When launched as the child, just write forever until killed.
	if os.Getenv("KV_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestEngineCrashRecovery")
	cmd.Env = append(os.Environ(), "KV_CRASH_CHILD=1", "KV_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	var ackedKeys []string
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "ACK ") {
			ackedKeys = append(ackedKeys, strings.TrimPrefix(line, "ACK "))
		}
		if len(ackedKeys) >= 200 {
			break
		}
	}

	cmd.Process.Kill() // hard kill, no cleanup runs in the child
	cmd.Wait()

	if len(ackedKeys) < 200 {
		t.Fatalf("child only acknowledged %d writes", len(ackedKeys))
	}

	db, err := OpenDB(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, key := range ackedKeys {
		if _, found, _ := db.Get([]byte(key)); !found {
			t.Fatalf("acknowledged key %s lost after crash", key)
		}
	}
	t.Logf("child acked %d writes before kill; all recovered", len(ackedKeys))
}

func runCrashChild() {
	db, err := OpenDB(os.Getenv("KV_DIR"), true)
	if err != nil {
		os.Exit(1)
	}
	for i := 0; ; i++ {
		key := fmt.Sprintf("key%06d", i)
		if err := db.Put([]byte(key), []byte("value")); err != nil {
			os.Exit(1)
		}
		fmt.Println("ACK " + key) // only printed after the write is durable
	}
}
