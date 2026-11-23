package storage

import (
	"fmt"
	"math"
	"testing"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	filter := NewBloomFilter(10000, 0.01)
	for i := 0; i < 10000; i++ {
		filter.Add([]byte(fmt.Sprintf("key%d", i)))
	}

	decoded, err := DecodeBloomFilter(filter.Encode())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if !filter.MayContain(key) || !decoded.MayContain(key) {
			t.Fatalf("false negative for %s", key)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	n := 10000
	target := 0.01
	filter := NewBloomFilter(n, target)
	for i := 0; i < n; i++ {
		filter.Add([]byte(fmt.Sprintf("key%d", i)))
	}

	probes := 100000
	falsePositives := 0
	for i := 0; i < probes; i++ {
		if filter.MayContain([]byte(fmt.Sprintf("absent%d", i))) {
			falsePositives++
		}
	}

	measured := float64(falsePositives) / float64(probes)
	k := float64(filter.numHashes)
	m := float64(filter.numBits)
	theoretical := math.Pow(1-math.Exp(-k*float64(n)/m), k)

	t.Logf("m=%d bits (%.1f bits/key), k=%d hashes", filter.numBits, m/float64(n), filter.numHashes)
	t.Logf("theoretical FP=%.4f%%  measured FP=%.4f%%", theoretical*100, measured*100)

	if measured > 2*target {
		t.Fatalf("FP rate %.4f too high (target %.4f)", measured, target)
	}
}
