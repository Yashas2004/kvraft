package storage

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits      []byte
	numBits   uint32
	numHashes uint32
}

// NewBloomFilter sizes the filter for n items at the target false-positive rate.
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	n := float64(expectedItems)
	if n < 1 {
		n = 1
	}

	m := math.Ceil(-n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	k := math.Round(m / n * math.Ln2)
	if k < 1 {
		k = 1
	}

	numBits := uint32(m)
	return &BloomFilter{
		bits:      make([]byte, (numBits+7)/8),
		numBits:   numBits,
		numHashes: uint32(k),
	}
}

// hashKey returns two independent-looking 32-bit hashes from one 64-bit hash.
func hashKey(key []byte) (uint32, uint32) {
	hasher := fnv.New64a()
	hasher.Write(key)
	h := hasher.Sum64()

	// Finalizer from MurmurHash3: spreads every input bit across the output.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33

	h1 := uint32(h)
	h2 := uint32(h >> 32)
	if h2 == 0 {
		h2 = 1 // otherwise every probe would hit the same bit
	}
	return h1, h2
}

func (b *BloomFilter) bitPosition(h1, h2 uint32, i uint32) uint32 {
	combined := uint64(h1) + uint64(i)*uint64(h2)
	return uint32(combined % uint64(b.numBits))
}

func (b *BloomFilter) Add(key []byte) {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < b.numHashes; i++ {
		bit := b.bitPosition(h1, h2, i)
		b.bits[bit/8] |= 1 << (bit % 8) // byte index, then bit inside that byte
	}
}

// MayContain: false means definitely absent; true means probably present.
func (b *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < b.numHashes; i++ {
		bit := b.bitPosition(h1, h2, i)
		if b.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode layout: numBits (4B) | numHashes (4B) | bit array
func (b *BloomFilter) Encode() []byte {
	buf := make([]byte, 8+len(b.bits))
	binary.LittleEndian.PutUint32(buf[0:4], b.numBits)
	binary.LittleEndian.PutUint32(buf[4:8], b.numHashes)
	copy(buf[8:], b.bits)
	return buf
}

func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, errors.New("bloom: data too short")
	}
	numBits := binary.LittleEndian.Uint32(data[0:4])
	numHashes := binary.LittleEndian.Uint32(data[4:8])
	if numBits == 0 || uint32(len(data)-8) != (numBits+7)/8 {
		return nil, errors.New("bloom: corrupt header")
	}

	bits := make([]byte, len(data)-8)
	copy(bits, data[8:])
	return &BloomFilter{bits: bits, numBits: numBits, numHashes: numHashes}, nil
}
