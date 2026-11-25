package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	sstableMagic    uint64 = 0x6B767261667400 // "kvraft" tag to detect wrong/corrupt files
	footerSize             = 24               // indexOffset(8) + bloomOffset(8) + magic(8)
	indexInterval          = 16               // one index entry per 16 data entries
	entryHeaderSize        = 9                // keyLen(4) + valLen(4) + type(1)
)

type indexEntry struct {
	key    []byte
	offset uint64
}

type SSTable struct {
	file    *os.File
	index   []indexEntry
	bloom   *BloomFilter
	dataEnd uint64 // data section ends where the index begins
}

func encodeEntry(e Entry) []byte {
	buf := make([]byte, entryHeaderSize+len(e.Key)+len(e.Value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(e.Value)))
	buf[8] = recordPut
	if e.Deleted {
		buf[8] = recordDelete
	}
	copy(buf[9:], e.Key)
	copy(buf[9+len(e.Key):], e.Value)
	return buf
}

// WriteSSTable writes entries (must already be sorted) to path atomically.
func WriteSSTable(path string, entries []Entry) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(f)
	bloom := NewBloomFilter(len(entries), 0.01)
	var indexBuf []byte
	var offset uint64

	for i, e := range entries {
		if i%indexInterval == 0 {
			item := make([]byte, 4+len(e.Key)+8)
			binary.LittleEndian.PutUint32(item[0:4], uint32(len(e.Key)))
			copy(item[4:], e.Key)
			binary.LittleEndian.PutUint64(item[4+len(e.Key):], offset)
			indexBuf = append(indexBuf, item...)
		}
		bloom.Add(e.Key)

		data := encodeEntry(e)
		writer.Write(data)
		offset += uint64(len(data))
	}

	indexOffset := offset
	writer.Write(indexBuf)
	offset += uint64(len(indexBuf))

	bloomOffset := offset
	writer.Write(bloom.Encode())

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], bloomOffset)
	binary.LittleEndian.PutUint64(footer[16:24], sstableMagic)
	writer.Write(footer)

	// bufio.Writer errors are "sticky": if any Write above failed, Flush returns that error.
	if err := writer.Flush(); err != nil {
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
	return os.Rename(tmpPath, path) // atomic: file appears complete or not at all
}

func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			f.Close() // close on any error path below
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := uint64(info.Size())
	if size < footerSize {
		return nil, errors.New("sstable: file too small")
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, int64(size-footerSize)); err != nil {
		return nil, err
	}
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomOffset := binary.LittleEndian.Uint64(footer[8:16])
	magic := binary.LittleEndian.Uint64(footer[16:24])
	if magic != sstableMagic || indexOffset > bloomOffset || bloomOffset > size-footerSize {
		return nil, errors.New("sstable: corrupt footer")
	}

	indexData := make([]byte, bloomOffset-indexOffset)
	if _, err := f.ReadAt(indexData, int64(indexOffset)); err != nil {
		return nil, err
	}
	index, err := parseIndex(indexData)
	if err != nil {
		return nil, err
	}

	bloomData := make([]byte, size-footerSize-bloomOffset)
	if _, err := f.ReadAt(bloomData, int64(bloomOffset)); err != nil {
		return nil, err
	}
	bloom, err := DecodeBloomFilter(bloomData)
	if err != nil {
		return nil, err
	}

	success = true
	return &SSTable{file: f, index: index, bloom: bloom, dataEnd: indexOffset}, nil
}

func parseIndex(data []byte) ([]indexEntry, error) {
	var index []indexEntry
	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			return nil, errors.New("sstable: corrupt index")
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+keyLen+8 > len(data) {
			return nil, errors.New("sstable: corrupt index")
		}
		key := data[pos : pos+keyLen]
		pos += keyLen
		offset := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		index = append(index, indexEntry{key: key, offset: offset})
	}
	return index, nil
}

// Get returns (value, deleted, found, err). Bloom filter first: a "no" skips the disk entirely.
func (s *SSTable) Get(key []byte) ([]byte, bool, bool, error) {
	if !s.bloom.MayContain(key) {
		return nil, false, false, nil
	}
	return s.lookup(key)
}

// lookup is the disk path: binary search the sparse index, read one block, scan it.
func (s *SSTable) lookup(key []byte) ([]byte, bool, bool, error) {
	// Find the last index entry whose key is <= the target.
	lo, hi := 0, len(s.index)-1
	blockIndex := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if bytes.Compare(s.index[mid].key, key) <= 0 {
			blockIndex = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if blockIndex == -1 {
		return nil, false, false, nil // smaller than every key in the file
	}

	start := s.index[blockIndex].offset
	end := s.dataEnd
	if blockIndex+1 < len(s.index) {
		end = s.index[blockIndex+1].offset
	}

	block := make([]byte, end-start)
	if _, err := s.file.ReadAt(block, int64(start)); err != nil {
		return nil, false, false, err
	}

	pos := 0
	for pos < len(block) {
		if pos+entryHeaderSize > len(block) {
			return nil, false, false, errors.New("sstable: corrupt block")
		}
		keyLen := int(binary.LittleEndian.Uint32(block[pos : pos+4]))
		valLen := int(binary.LittleEndian.Uint32(block[pos+4 : pos+8]))
		entryType := block[pos+8]
		keyStart := pos + entryHeaderSize
		valStart := keyStart + keyLen
		if valStart+valLen > len(block) {
			return nil, false, false, errors.New("sstable: corrupt block")
		}

		entryKey := block[keyStart:valStart]
		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
			value := block[valStart : valStart+valLen]
			return value, entryType == recordDelete, true, nil
		}
		if cmp > 0 {
			break // sorted: we've passed where the key would be
		}
		pos = valStart + valLen
	}
	return nil, false, false, nil
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

// sstIterator streams entries in key order from an SSTable's data section.
type sstIterator struct {
	reader  *bufio.Reader
	current Entry
	err     error
}

func (s *SSTable) newIterator() *sstIterator {
	section := io.NewSectionReader(s.file, 0, int64(s.dataEnd))
	return &sstIterator{reader: bufio.NewReader(section)}
}

// Next advances to the next entry. Returns false at the end or on error (check it.err).
func (it *sstIterator) Next() bool {
	header := make([]byte, entryHeaderSize)
	_, err := io.ReadFull(it.reader, header)
	if err == io.EOF {
		return false // clean end of data section
	}
	if err != nil {
		it.err = err
		return false
	}

	keyLen := int(binary.LittleEndian.Uint32(header[0:4]))
	valLen := int(binary.LittleEndian.Uint32(header[4:8]))
	body := make([]byte, keyLen+valLen)
	if _, err := io.ReadFull(it.reader, body); err != nil {
		it.err = err
		return false
	}

	it.current = Entry{
		Key:     body[:keyLen],
		Value:   body[keyLen:],
		Deleted: header[8] == recordDelete,
	}
	return true
}

func (it *sstIterator) Current() Entry { return it.current }
func (it *sstIterator) Err() error     { return it.err }
