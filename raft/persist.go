package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	recEntry    byte = 1
	recTruncate byte = 2
	// crc(4) | type(1) | index(8) | term(8) | cmdLen(4)
	logRecordHeader = 25
	maxCommandSize  = 64 * 1024 * 1024
)

type PersistedState struct {
	Term          int
	VotedFor      int
	SnapshotIndex int
	SnapshotTerm  int
	Snapshot      []byte
	Log           []LogEntry // Log[0] is a placeholder holding SnapshotTerm
}

// Persister stores Raft's persistent state in three files, each with its own lock
// so that, e.g., a vote never waits behind a log compaction:
//
//	raft-state    (stateMu): currentTerm + votedFor, atomic replace
//	raft-log      (mu):      append-only CRC-checked records
//	raft-snapshot (snapMu):  index, term, service snapshot, atomic replace
type Persister struct {
	mu                 sync.Mutex
	stateMu            sync.Mutex
	snapMu             sync.Mutex
	dir                string
	logFile            *os.File
	closed             atomic.Bool
	savedSnapshotIndex int // highest snapshot index durable on disk (snapMu)
}

func OpenPersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	p := &Persister{dir: dir}
	f, err := os.OpenFile(p.logPath(), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	p.logFile = f
	return p, nil
}

func (p *Persister) statePath() string    { return filepath.Join(p.dir, "raft-state") }
func (p *Persister) logPath() string      { return filepath.Join(p.dir, "raft-log") }
func (p *Persister) snapshotPath() string { return filepath.Join(p.dir, "raft-snapshot") }

func writeSynced(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// renameWithRetry: on Windows, a rename fails with "Access is denied" while any
// other handle (antivirus, indexer, a reader) has the target open. Retry briefly.
func renameWithRetry(from, to string) error {
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		err = os.Rename(from, to)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return err
}

func writeFileAtomic(path string, data []byte) error {
	tmpPath := path + ".tmp"
	if err := writeSynced(tmpPath, data); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func encodeLogRecord(recType byte, index, term int, command []byte) []byte {
	buf := make([]byte, logRecordHeader+len(command))
	buf[4] = recType
	binary.LittleEndian.PutUint64(buf[5:13], uint64(index))
	binary.LittleEndian.PutUint64(buf[13:21], uint64(term))
	binary.LittleEndian.PutUint32(buf[21:25], uint32(len(command)))
	copy(buf[25:], command)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

func encodeSnapshotFile(index, term int, data []byte) []byte {
	buf := make([]byte, 20+len(data))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(index))
	binary.LittleEndian.PutUint64(buf[12:20], uint64(term))
	copy(buf[20:], data)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// replayLog reads raft-log records and rebuilds the log after base.
// Records at or below base are covered by the snapshot and skipped.
// Returns the log (with placeholder) and the byte size of the valid prefix.
func replayLog(r io.Reader, base, baseTerm int) ([]LogEntry, int64, error) {
	reader := bufio.NewReader(r)
	header := make([]byte, logRecordHeader)
	log := []LogEntry{{Term: baseTerm}}
	var validSize int64

	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // clean end or torn header
		}
		if err != nil {
			return nil, 0, err
		}

		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		recType := header[4]
		index := int(binary.LittleEndian.Uint64(header[5:13]))
		entryTerm := int(binary.LittleEndian.Uint64(header[13:21]))
		cmdLen := int(binary.LittleEndian.Uint32(header[21:25]))
		if cmdLen > maxCommandSize {
			break
		}
		command := make([]byte, cmdLen)
		if _, err := io.ReadFull(reader, command); err != nil {
			break // torn body
		}
		hasher := crc32.NewIEEE()
		hasher.Write(header[4:])
		hasher.Write(command)
		if hasher.Sum32() != storedCRC {
			break // torn or corrupt record
		}

		if recType == recTruncate {
			if index <= base {
				log = log[:1]
			} else if index-base < len(log) {
				log = log[:index-base]
			}
		} else if recType == recEntry {
			if index <= base {
				// covered by the snapshot
			} else if index == base+len(log) {
				log = append(log, LogEntry{Term: entryTerm, Command: command})
			} else {
				break // gap: corrupt
			}
		}
		validSize += int64(logRecordHeader + cmdLen)
	}
	return log, validSize, nil
}

func (p *Persister) LoadState() (PersistedState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := PersistedState{VotedFor: -1}

	data, err := os.ReadFile(p.statePath())
	if err == nil && len(data) == 16 {
		state.Term = int(binary.LittleEndian.Uint64(data[0:8]))
		state.VotedFor = int(int64(binary.LittleEndian.Uint64(data[8:16])))
	} else if err != nil && !os.IsNotExist(err) {
		return state, err
	}

	snapData, err := os.ReadFile(p.snapshotPath())
	if err == nil {
		if len(snapData) < 20 {
			return state, errors.New("raft: snapshot file too short")
		}
		if crc32.ChecksumIEEE(snapData[4:]) != binary.LittleEndian.Uint32(snapData[0:4]) {
			return state, errors.New("raft: snapshot checksum mismatch")
		}
		state.SnapshotIndex = int(binary.LittleEndian.Uint64(snapData[4:12]))
		state.SnapshotTerm = int(binary.LittleEndian.Uint64(snapData[12:20]))
		state.Snapshot = make([]byte, len(snapData)-20)
		copy(state.Snapshot, snapData[20:])
	} else if !os.IsNotExist(err) {
		return state, err
	}

	p.snapMu.Lock()
	p.savedSnapshotIndex = state.SnapshotIndex
	p.snapMu.Unlock()

	if _, err := p.logFile.Seek(0, io.SeekStart); err != nil {
		return state, err
	}
	log, validSize, err := replayLog(p.logFile, state.SnapshotIndex, state.SnapshotTerm)
	if err != nil {
		return state, err
	}
	if err := p.logFile.Truncate(validSize); err != nil {
		return state, err
	}
	if _, err := p.logFile.Seek(validSize, io.SeekStart); err != nil {
		return state, err
	}
	state.Log = log
	return state, nil
}

func (p *Persister) SaveTermVote(term, votedFor int) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.closed.Load() {
		return nil
	}
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(int64(votedFor)))
	return writeFileAtomic(p.statePath(), buf)
}

func (p *Persister) AppendLog(fromIndex int, entries []LogEntry, truncate bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.appendLocked(fromIndex, entries, truncate)
}

// appendLocked is AppendLog for callers that already hold p.mu (the leader syncer).
func (p *Persister) appendLocked(fromIndex int, entries []LogEntry, truncate bool) error {
	if p.closed.Load() {
		return nil
	}
	var buf []byte
	if truncate {
		buf = append(buf, encodeLogRecord(recTruncate, fromIndex, 0, nil)...)
	}
	for i, e := range entries {
		buf = append(buf, encodeLogRecord(recEntry, fromIndex+i, e.Term, e.Command)...)
	}
	if _, err := p.logFile.Write(buf); err != nil {
		return err
	}
	return p.logFile.Sync()
}

// rewriteLogLocked atomically replaces raft-log with data. Caller holds p.mu.
func (p *Persister) rewriteLogLocked(data []byte) error {
	tmpPath := p.logPath() + ".tmp"
	if err := writeSynced(tmpPath, data); err != nil {
		return err
	}
	if err := p.logFile.Close(); err != nil { // Windows: can't rename over an open file
		return err
	}
	if err := os.Rename(tmpPath, p.logPath()); err != nil {
		return err
	}
	f, err := os.OpenFile(p.logPath(), os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	p.logFile = f
	return nil
}

// SaveSnapshotFile durably writes only the snapshot file (background saver).
// Never replaces a newer snapshot with an older one.
func (p *Persister) SaveSnapshotFile(index, term int, data []byte) error {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	if p.closed.Load() || index <= p.savedSnapshotIndex {
		return nil
	}
	if err := writeFileAtomic(p.snapshotPath(), encodeSnapshotFile(index, term, data)); err != nil {
		return err
	}
	p.savedSnapshotIndex = index
	return nil
}

// SaveSnapshot writes the snapshot AND rewrites raft-log to hold exactly
// entries (index+1 onward). Used by InstallSnapshot, which may discard a
// conflicting log tail, so the old log must not survive.
func (p *Persister) SaveSnapshot(index, term int, data []byte, entries []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	if p.closed.Load() {
		return nil
	}

	if err := writeFileAtomic(p.snapshotPath(), encodeSnapshotFile(index, term, data)); err != nil {
		return err
	}
	if index > p.savedSnapshotIndex {
		p.savedSnapshotIndex = index
	}

	var logBuf []byte
	for i, e := range entries {
		logBuf = append(logBuf, encodeLogRecord(recEntry, index+1+i, e.Term, e.Command)...)
	}
	return p.rewriteLogLocked(logBuf)
}

// CompactLog rewrites raft-log without the records a DURABLE snapshot covers.
func (p *Persister) CompactLog(base int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil
	}

	p.snapMu.Lock()
	if p.savedSnapshotIndex < base {
		base = p.savedSnapshotIndex // only drop what is safely covered on disk
	}
	p.snapMu.Unlock()
	if base == 0 {
		return nil
	}

	if _, err := p.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	log, _, err := replayLog(p.logFile, base, 0)
	if err != nil {
		return err
	}
	var buf []byte
	for i := 1; i < len(log); i++ {
		buf = append(buf, encodeLogRecord(recEntry, base+i, log[i].Term, log[i].Command)...)
	}
	return p.rewriteLogLocked(buf)
}

func (p *Persister) LogFileSize() int64 {
	info, err := os.Stat(p.logPath())
	if err != nil {
		return 0
	}
	return info.Size()
}

func (p *Persister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Swap(true) {
		return nil
	}
	return p.logFile.Close()
}
