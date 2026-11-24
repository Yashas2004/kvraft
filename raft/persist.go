package raft

import (
	"bufio"
	"encoding/binary"
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

// Persister stores Raft's persistent state (Figure 2):
//
//	raft-state: currentTerm + votedFor, replaced atomically (shadow copy + rename)
//	raft-log:   append-only, CRC-checked records; truncation is itself a record
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
	f, err := os.OpenFile(filepath.Join(dir, "raft-log"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &Persister{dir: dir, logFile: f}, nil
}

func (p *Persister) statePath() string {
	return filepath.Join(p.dir, "raft-state")
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

// LoadState returns (currentTerm, votedFor, log). The log includes the dummy entry at index 0.
func (p *Persister) LoadState() (int, int, []LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	term, votedFor := 0, -1
	data, err := os.ReadFile(p.statePath())
	if err == nil && len(data) == 16 {
		term = int(binary.LittleEndian.Uint64(data[0:8]))
		votedFor = int(int64(binary.LittleEndian.Uint64(data[8:16])))
	} else if err != nil && !os.IsNotExist(err) {
		return 0, 0, nil, err
	}

	log := []LogEntry{{Term: 0}}
	if _, err := p.logFile.Seek(0, io.SeekStart); err != nil {
		return 0, 0, nil, err
	}
	reader := bufio.NewReader(p.logFile)
	header := make([]byte, logRecordHeader)
	var validSize int64

	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // clean end or torn header
		}
		if err != nil {
			return 0, 0, nil, err
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
			if index >= 1 && index < len(log) {
				log = log[:index] // drop index and everything after
			}
		} else if recType == recEntry {
			if index != len(log) {
				break // entries must be contiguous
			}
			log = append(log, LogEntry{Term: entryTerm, Command: command})
		}
		validSize += int64(logRecordHeader + cmdLen)
	}

	// Cut off any torn tail and continue appending after the last good record.
	if err := p.logFile.Truncate(validSize); err != nil {
		return 0, 0, nil, err
	}
	if _, err := p.logFile.Seek(validSize, io.SeekStart); err != nil {
		return 0, 0, nil, err
	}
	return term, votedFor, log, nil
}

// SaveTermVote replaces raft-state atomically: write a temp file, fsync, rename.
func (p *Persister) SaveTermVote(term, votedFor int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil // node has "crashed"; writes after that are lost, like a power cut
	}

	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(int64(votedFor)))

	tmpPath := p.statePath() + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, p.statePath())
}

// AppendLog durably records entries starting at fromIndex. If truncate is true,
// everything at fromIndex and after is discarded first. One fsync per call.
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

func (p *Persister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.logFile.Close()
}
