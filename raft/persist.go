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
)

const (
	recEntry    byte = 1
	recTruncate byte = 2
	// crc(4) | type(1) | index(8) | term(8) | cmdLen(4)
	logRecordHeader = 25
	maxCommandSize  = 64 * 1024 * 1024
)

// PersistedState is everything Raft needs to restart.
type PersistedState struct {
	Term          int
	VotedFor      int
	SnapshotIndex int // last log index covered by the snapshot (0 = none)
	SnapshotTerm  int
	Snapshot      []byte
	Log           []LogEntry // Log[0] is a placeholder holding SnapshotTerm
}

// Persister stores Raft's persistent state:
//
//	raft-state:    currentTerm + votedFor (shadow copy + atomic rename)
//	raft-log:      append-only CRC-checked records; truncation is a record
//	raft-snapshot: index, term and the service's snapshot bytes (atomic rename)
type Persister struct {
	mu      sync.Mutex
	dir     string
	logFile *os.File
	closed  bool
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

// writeSynced writes data to path and fsyncs it.
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

// writeFileAtomic: the file at path is either the old version or the new one, never half.
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

	// Snapshot file: crc(4) | index(8) | term(8) | data
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

	base := state.SnapshotIndex
	log := []LogEntry{{Term: state.SnapshotTerm}}

	if _, err := p.logFile.Seek(0, io.SeekStart); err != nil {
		return state, err
	}
	reader := bufio.NewReader(p.logFile)
	header := make([]byte, logRecordHeader)
	var validSize int64

	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return state, err
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
			break
		}
		hasher := crc32.NewIEEE()
		hasher.Write(header[4:])
		hasher.Write(command)
		if hasher.Sum32() != storedCRC {
			break
		}

		if recType == recTruncate {
			if index <= base {
				log = log[:1] // drops everything after the snapshot
			} else if index-base < len(log) {
				log = log[:index-base]
			}
		} else if recType == recEntry {
			if index <= base {
				// Covered by the snapshot (crash between snapshot save and log rewrite).
			} else if index == base+len(log) {
				log = append(log, LogEntry{Term: entryTerm, Command: command})
			} else {
				break // gap: corrupt
			}
		}
		validSize += int64(logRecordHeader + cmdLen)
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
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
	if p.closed {
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

// SaveSnapshot stores the snapshot, then rewrites raft-log to hold only the
// entries after it. Order matters: a crash between the two steps leaves a new
// snapshot plus the old log, and LoadState skips records the snapshot covers.
func (p *Persister) SaveSnapshot(index, term int, data []byte, entries []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}

	snap := make([]byte, 20+len(data))
	binary.LittleEndian.PutUint64(snap[4:12], uint64(index))
	binary.LittleEndian.PutUint64(snap[12:20], uint64(term))
	copy(snap[20:], data)
	binary.LittleEndian.PutUint32(snap[0:4], crc32.ChecksumIEEE(snap[4:]))
	if err := writeFileAtomic(p.snapshotPath(), snap); err != nil {
		return err
	}

	var logBuf []byte
	for i, e := range entries {
		logBuf = append(logBuf, encodeLogRecord(recEntry, index+1+i, e.Term, e.Command)...)
	}
	tmpPath := p.logPath() + ".tmp"
	if err := writeSynced(tmpPath, logBuf); err != nil {
		return err
	}
	// Windows can't rename over an open file: close, swap, reopen.
	if err := p.logFile.Close(); err != nil {
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

func (p *Persister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.logFile.Close()
}
