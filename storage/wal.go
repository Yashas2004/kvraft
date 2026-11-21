package storage

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	recordPut     byte = 1
	recordDelete  byte = 2
	headerSize         = 13               // crc(4) + keyLen(4) + valLen(4) + type(1)
	maxRecordPart      = 64 * 1024 * 1024 // sanity cap so a corrupt length can't make us allocate GBs
)

type Record struct {
	Type  byte
	Key   []byte
	Value []byte
}

type WAL struct {
	mu            sync.Mutex
	file          *os.File
	syncEachWrite bool
}

// OpenWAL opens (or creates) the log, replays valid records,
// truncates any torn tail, and returns the WAL ready for appends.
func OpenWAL(path string, syncEachWrite bool) (*WAL, []Record, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}

	records, validSize, err := readRecords(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	// Throw away the half-written record (if any) from the last crash.
	if err := f.Truncate(validSize); err != nil {
		f.Close()
		return nil, nil, err
	}
	// Future writes continue right after the last good record.
	if _, err := f.Seek(validSize, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}

	wal := &WAL{file: f, syncEachWrite: syncEachWrite}
	return wal, records, nil
}

func encodeRecord(r Record) []byte {
	buf := make([]byte, headerSize+len(r.Key)+len(r.Value))

	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(r.Key)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(r.Value)))
	buf[12] = r.Type
	copy(buf[13:], r.Key)
	copy(buf[13+len(r.Key):], r.Value)

	// CRC covers everything after the CRC field itself.
	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)
	return buf
}

func (w *WAL) Append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := encodeRecord(r)
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	if w.syncEachWrite {
		return w.file.Sync()
	}
	return nil
}

func readRecords(f *os.File) ([]Record, int64, error) {
	reader := bufio.NewReader(f)
	var records []Record
	var offset int64
	header := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // clean end of file, or a torn header
		}
		if err != nil {
			return nil, 0, err
		}

		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		keyLen := binary.LittleEndian.Uint32(header[4:8])
		valLen := binary.LittleEndian.Uint32(header[8:12])
		if keyLen > maxRecordPart || valLen > maxRecordPart {
			break // garbage lengths = corrupt tail
		}

		body := make([]byte, keyLen+valLen)
		_, err = io.ReadFull(reader, body)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // torn body
		}
		if err != nil {
			return nil, 0, err
		}

		hasher := crc32.NewIEEE()
		hasher.Write(header[4:])
		hasher.Write(body)
		if hasher.Sum32() != storedCRC {
			break // torn or corrupted record: stop here
		}

		record := Record{
			Type:  header[12],
			Key:   body[:keyLen],
			Value: body[keyLen:],
		}
		records = append(records, record)
		offset += int64(headerSize) + int64(keyLen) + int64(valLen)
	}

	return records, offset, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
