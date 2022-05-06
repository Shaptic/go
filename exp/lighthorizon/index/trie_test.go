package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tchap/go-patricia/patricia"
)

func randomTrie(t *testing.T, index *TrieIndex) (*TrieIndex, map[string]uint32) {
	if index == nil {
		index = &TrieIndex{}
	}
	inserts := map[string]uint32{}
	numInserts := rand.Intn(100)
	for j := 0; j < numInserts; j++ {
		ledger := uint32(rand.Int63())
		hashBytes := make([]byte, 32)
		rand.Read(hashBytes) // never fails
		hash := hex.EncodeToString(hashBytes)

		inserts[hash] = ledger
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, ledger)
		index.Upsert([]byte(hash), b)
	}
	return index, inserts
}

func TestTrieIndex(t *testing.T) {
	for i := 0; i < 10_000; i++ {
		index, inserts := randomTrie(t, nil)

		for key, expected := range inserts {
			value, ok := index.Get([]byte(key))
			if !ok {
				t.Errorf("Key not found: %s", key)
			} else {
				ledger := binary.BigEndian.Uint32(value)
				if ledger != expected {
					t.Errorf("Key %s found: %v, expected: %v", key, ledger, expected)
				}
			}
		}
	}
}

func TestTrieIndexUpsertBasic(t *testing.T) {
	index := &TrieIndex{}

	key := "key"
	prev, ok := index.Upsert([]byte(key), []byte("a"))
	if ok || prev != nil {
		t.Errorf("Unexpected previous value: %q, expected: nil", string(prev))
	}

	prev, ok = index.Upsert([]byte(key), []byte("b"))
	if !ok || string(prev) != "a" {
		t.Errorf("Unexpected previous value: %q, expected: a", string(prev))
	}

	prev, ok = index.Upsert([]byte(key), []byte("c"))
	if !ok || string(prev) != "b" {
		t.Errorf("Unexpected previous value: %q, expected: b", string(prev))
	}
}

func TestTrieIndexSuffixes(t *testing.T) {
	index := &TrieIndex{}

	prev, ok := index.Upsert([]byte("a"), []byte("a"))
	require.False(t, ok)
	require.Nil(t, prev)

	prev, ok = index.Upsert([]byte("ab"), []byte("ab"))
	require.False(t, ok)
	require.Nil(t, prev)

	prev, ok = index.Get([]byte("a"))
	require.True(t, ok)
	require.Equal(t, "a", string(prev))

	prev, ok = index.Get([]byte("ab"))
	require.True(t, ok)
	require.Equal(t, "ab", string(prev))

	prev, ok = index.Upsert([]byte("a"), []byte("b"))
	require.True(t, ok)
	require.Equal(t, "a", string(prev))

	prev, ok = index.Get([]byte("a"))
	require.True(t, ok)
	require.Equal(t, "b", string(prev))
}

func TestTrieIndexSerialization(t *testing.T) {
	for i := 0; i < 10_000; i++ {
		index, inserts := randomTrie(t, nil)

		// Round-trip it to serialization and back
		buf := &bytes.Buffer{}
		nWritten, err := index.WriteTo(buf)
		if err != nil {
			t.Error(err.Error())
		}

		read := &TrieIndex{}
		nRead, err := read.ReadFrom(buf)
		if err != nil {
			t.Error(err.Error())
		}

		if nWritten != nRead {
			t.Errorf("Wrote %d bytes, but read %d bytes", nWritten, nRead)
		}

		for key, expected := range inserts {
			value, ok := read.Get([]byte(key))
			if !ok {
				t.Errorf("Key not found: %s", key)
			} else {
				ledger := binary.BigEndian.Uint32(value)
				if ledger != expected {
					t.Errorf("Key %s found: %v, expected: %v", key, ledger, expected)
				}
			}
		}
	}
}

func requireEqualNodes(t *testing.T, expectedNode, gotNode *trieNode) {
	expectedJSON, err := json.Marshal(expectedNode)
	require.NoError(t, err)
	expected := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(expectedJSON, &expected))

	gotJSON, err := json.Marshal(gotNode)
	require.NoError(t, err)
	got := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(gotJSON, &got))

	require.Equal(t, expected, got)
}

func TestTrieIndexUpsertAdvanced(t *testing.T) {
	// TODO: This is janky that we inspect the structure, but I want to make sure
	// I've gotten the algorithms correct.
	makeBase := func() *TrieIndex {
		index := &TrieIndex{}
		index.Upsert([]byte("annibale"), []byte{1})
		index.Upsert([]byte("annibalesco"), []byte{2})
		return index
	}

	t.Run("base", func(t *testing.T) {
		base := makeBase()

		baseExpected := &trieNode{
			Prefix: []byte("annibale"),
			Value:  []byte{1},
			Children: map[byte]*trieNode{
				byte('s'): {
					Prefix: []byte("co"),
					Value:  []byte{2},
				},
			},
		}
		requireEqualNodes(t, baseExpected, base.Root)
	})

	for _, tc := range []struct {
		key      string
		expected *trieNode
	}{
		{"annientare", &trieNode{
			Prefix: []byte("anni"),
			Children: map[byte]*trieNode{
				'b': {
					Prefix: []byte("ale"),
					Value:  []byte{1},
					Children: map[byte]*trieNode{
						's': {
							Prefix: []byte("co"),
							Value:  []byte{2},
						},
					},
				},
				'e': {
					Prefix: []byte("ntare"),
					Value:  []byte{3},
				},
			},
		}},
		{"annibali", &trieNode{
			Prefix: []byte("annibal"),
			Children: map[byte]*trieNode{
				'e': {
					Value: []byte{1},
					Children: map[byte]*trieNode{
						's': {
							Prefix: []byte("co"),
							Value:  []byte{2},
						},
					},
				},
				'i': {
					Value: []byte{3},
				},
			},
		}},
		{"ago", &trieNode{
			Prefix: []byte("a"),
			Children: map[byte]*trieNode{
				'n': {
					Prefix: []byte("nibale"),
					Value:  []byte{1},
					Children: map[byte]*trieNode{
						's': {
							Prefix: []byte("co"),
							Value:  []byte{2},
						},
					},
				},
				'g': {
					Prefix: []byte("o"),
					Value:  []byte{3},
				},
			},
		}},
		{"ciao", &trieNode{
			Children: map[byte]*trieNode{
				'a': {
					Prefix: []byte("nnibale"),
					Value:  []byte{1},
					Children: map[byte]*trieNode{
						's': {
							Prefix: []byte("co"),
							Value:  []byte{2},
						},
					},
				},
				'c': {
					Prefix: []byte("iao"),
					Value:  []byte{3},
				},
			},
		}},
		{"anni", &trieNode{
			Prefix: []byte("anni"),
			Value:  []byte{3},
			Children: map[byte]*trieNode{
				'b': {
					Prefix: []byte("ale"),
					Value:  []byte{1},
					Children: map[byte]*trieNode{
						's': {
							Prefix: []byte("co"),
							Value:  []byte{2},
						},
					},
				},
			},
		}},
	} {
		t.Run(tc.key, func(t *testing.T) {
			// Do our upsert
			index := makeBase()
			index.Upsert([]byte(tc.key), []byte{3})

			// Check the tree is shaped right
			requireEqualNodes(t, tc.expected, index.Root)

			// Check the value matches expected
			value, ok := index.Get([]byte(tc.key))
			require.True(t, ok)
			require.Equal(t, []byte{3}, value)
		})
	}
}

func TestTrieIndexMerge(t *testing.T) {
	for i := 0; i < 10_000; i++ {
		a, aInserts := randomTrie(t, nil)
		b, bInserts := randomTrie(t, nil)

		require.NoError(t, a.Merge(b))

		// Should still have all the A keys
		for key, expected := range aInserts {
			value, ok := a.Get([]byte(key))
			if !ok {
				t.Errorf("Key not found: %s", key)
			} else {
				ledger := binary.BigEndian.Uint32(value)
				if ledger != expected {
					t.Errorf("Key %s found: %v, expected: %v", key, ledger, expected)
				}
			}
		}

		// Should now also have all the B keys
		for key, expected := range bInserts {
			value, ok := a.Get([]byte(key))
			if !ok {
				t.Errorf("Key not found: %s", key)
			} else {
				ledger := binary.BigEndian.Uint32(value)
				if ledger != expected {
					t.Errorf("Key %s found: %v, expected: %v", key, ledger, expected)
				}
			}
		}
	}
}

func BenchmarkTrieOperations(b *testing.B) {
	b.Run("insert", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			makeBenchmarkedTrie(1000)
		}
	})

	b.Run("get", func(bb *testing.B) {
		randomBytes := make([]byte, 8*bb.N)
		rand.Read(randomBytes)
		trie := makeBenchmarkedTrie(10_000)
		bb.ResetTimer()

		for trial := 0; trial < bb.N; trial++ {
			word := randomBytes[8*trial : 8*trial+8]
			trie.Get(word)
		}
	})

	b.Run("merge", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bb.StopTimer()
			trie1, trie2 := makeBenchmarkedTrie(1000), makeBenchmarkedTrie(1000)
			bb.StartTimer()

			trie1.Merge(trie2)
		}
	})

	b.Run("serialization", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bb.StopTimer()
			trie := makeBenchmarkedTrie(10_000)
			bb.StartTimer()

			b := bytes.NewBuffer(nil)
			writer := bufio.NewWriter(b)
			trie.WriteTo(writer)

			b = bytes.NewBuffer(nil)
			reader := bufio.NewReader(b)
			trie.ReadFrom(reader)
		}
	})
}

func BenchmarkParticiaOperations(b *testing.B) {
	b.Run("insert", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			makeBenchmarkedPatricia(1000)
		}
	})

	b.Run("get", func(bb *testing.B) {
		randomBytes := make([]byte, 8*bb.N)
		rand.Read(randomBytes)
		trie := makeBenchmarkedPatricia(10_000)
		bb.ResetTimer()

		for trial := 0; trial < bb.N; trial++ {
			word := randomBytes[8*trial : 8*trial+8]
			trie.Get(word)
		}
	})

	b.Run("merge", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bb.StopTimer()
			trie1, trie2 := makeBenchmarkedPatricia(1000), makeBenchmarkedPatricia(1000)
			bb.StartTimer()

			trie1.Visit(patricia.VisitorFunc(
				func(prefix patricia.Prefix, item patricia.Item) (err error) {
					trie2.Insert(prefix, item)
					return nil
				}),
			)
		}
	})

	b.Run("serialization", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bb.StopTimer()
			trie := makeBenchmarkedPatricia(10_000)
			b := bytes.NewBuffer(nil)
			writer := bufio.NewWriter(b)
			bb.StartTimer()

			trie.Visit(patricia.VisitorFunc(
				func(prefix patricia.Prefix, item patricia.Item) (err error) {
					_, err = writer.Write(prefix)
					if err != nil {
						return err
					}
					_, err = writer.Write(item.([]byte))
					if err != nil {
						return err
					}
					return nil
				},
			))
		}
	})
}

func makeHellaEntries(count int) ([]string, [][]byte) {
	randomBytes := make([]byte, 32*count)
	rand.Read(randomBytes)

	randomHashes := make([]string, count)
	for i := 0; i < len(randomBytes); i += 32 {
		hash := hex.EncodeToString(randomBytes[i : i+32])
		randomHashes[i/32] = hash
	}

	randomKeys := make([][]byte, count)
	for i := 0; i < count; i++ {
		key := make([]byte, 4)
		randomKeys[i] = key
		ledger := uint32(rand.Int63())
		binary.BigEndian.PutUint32(key, ledger)
	}

	return randomHashes, randomKeys
}

func makeBenchmarkedTrie(count int) *TrieIndex {
	trie := &TrieIndex{}
	randomHashes, randomKeys := makeHellaEntries(count)

	for i := 0; i < len(randomHashes); i++ {
		hash, key := []byte(randomHashes[i]), randomKeys[i]
		trie.Upsert(key, hash)
	}

	return trie
}

func makeBenchmarkedPatricia(count int) *patricia.Trie {
	trie := patricia.NewTrie()
	randomHashes, randomKeys := makeHellaEntries(count)

	for i := 0; i < len(randomHashes); i++ {
		hash, key := []byte(randomHashes[i]), randomKeys[i]
		trie.Insert(key, hash)
	}

	return trie
}
