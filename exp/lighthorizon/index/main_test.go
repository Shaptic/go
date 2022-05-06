package index

import (
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromBytes(t *testing.T) {
	for i := uint32(1); i < 200; i++ {
		t.Run(fmt.Sprintf("New%d", i), func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(i)
			b := index.Flush()
			newIndex, err := NewCheckpointIndexFromBytes(b)
			require.NoError(t, err)
			assert.Equal(t, index.firstCheckpoint, newIndex.firstCheckpoint)
			assert.Equal(t, index.lastCheckpoint, newIndex.lastCheckpoint)
			assert.Equal(t, index.bitmap, newIndex.bitmap)
		})
	}
}

func TestSetActive(t *testing.T) {
	cases := []struct {
		checkpoint           uint32
		rangeFirstCheckpoint uint32
		bitmap               []byte
	}{
		{1, 1, []byte{0b1000_0000}},
		{2, 1, []byte{0b0100_0000}},
		{3, 1, []byte{0b0010_0000}},
		{4, 1, []byte{0b0001_0000}},
		{5, 1, []byte{0b0000_1000}},
		{6, 1, []byte{0b0000_0100}},
		{7, 1, []byte{0b0000_0010}},
		{8, 1, []byte{0b0000_0001}},

		{9, 9, []byte{0b1000_0000}},
		{10, 9, []byte{0b0100_0000}},
		{11, 9, []byte{0b0010_0000}},
		{12, 9, []byte{0b0001_0000}},
		{13, 9, []byte{0b0000_1000}},
		{14, 9, []byte{0b0000_0100}},
		{15, 9, []byte{0b0000_0010}},
		{16, 9, []byte{0b0000_0001}},
	}

	for _, tt := range cases {
		t.Run(fmt.Sprintf("init_%d", tt.checkpoint), func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(tt.checkpoint)

			assert.Equal(t, tt.bitmap, index.bitmap)
			assert.Equal(t, tt.rangeFirstCheckpoint, index.rangeFirstCheckpoint())
			assert.Equal(t, tt.checkpoint, index.firstCheckpoint)
			assert.Equal(t, tt.checkpoint, index.lastCheckpoint)
		})
	}

	// Update current bitmap right
	index := &CheckpointIndex{}
	index.SetActive(1)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(1), index.lastCheckpoint)
	index.SetActive(8)
	assert.Equal(t, []byte{0b1000_0001}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(8), index.lastCheckpoint)

	// Update current bitmap left
	index = &CheckpointIndex{}
	index.SetActive(8)
	assert.Equal(t, uint32(8), index.firstCheckpoint)
	assert.Equal(t, uint32(8), index.lastCheckpoint)
	index.SetActive(1)
	assert.Equal(t, []byte{0b1000_0001}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(8), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(10)
	index.SetActive(9)
	index.SetActive(16)
	assert.Equal(t, []byte{0b1100_0001}, index.bitmap)
	assert.Equal(t, uint32(9), index.firstCheckpoint)
	assert.Equal(t, uint32(16), index.lastCheckpoint)

	// Expand bitmap to the left
	index = &CheckpointIndex{}
	index.SetActive(10)
	index.SetActive(1)
	assert.Equal(t, []byte{0b1000_0000, 0b0100_0000}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(10), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(17)
	index.SetActive(2)
	assert.Equal(t, []byte{0b0100_0000, 0b0000_0000, 0b1000_0000}, index.bitmap)
	assert.Equal(t, uint32(2), index.firstCheckpoint)
	assert.Equal(t, uint32(17), index.lastCheckpoint)

	// Expand bitmap to the right
	index = &CheckpointIndex{}
	index.SetActive(1)
	index.SetActive(10)
	assert.Equal(t, []byte{0b1000_0000, 0b0100_0000}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(10), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(2)
	index.SetActive(17)
	assert.Equal(t, []byte{0b0100_0000, 0b0000_0000, 0b1000_0000}, index.bitmap)
	assert.Equal(t, uint32(2), index.firstCheckpoint)
	assert.Equal(t, uint32(17), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(17)
	index.SetActive(26)
	assert.Equal(t, []byte{0b1000_0000, 0b0100_0000}, index.bitmap)
	assert.Equal(t, uint32(17), index.firstCheckpoint)
	assert.Equal(t, uint32(26), index.lastCheckpoint)
}

func TestNextActive(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		index := &CheckpointIndex{}

		i, err := index.NextActive(0)
		assert.Equal(t, uint32(0), i)
		assert.EqualError(t, err, io.EOF.Error())
	})

	t.Run("one byte", func(t *testing.T) {
		t.Run("after last", func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(3)

			// 16 is well-past the end
			i, err := index.NextActive(16)
			assert.Equal(t, uint32(0), i)
			assert.EqualError(t, err, io.EOF.Error())
		})

		t.Run("only one bit in the byte", func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(1)

			i, err := index.NextActive(1)
			assert.NoError(t, err)
			assert.Equal(t, uint32(1), i)
		})

		t.Run("only one bit in the byte (offset)", func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(9)

			i, err := index.NextActive(1)
			assert.NoError(t, err)
			assert.Equal(t, uint32(9), i)
		})

		severalSet := &CheckpointIndex{}
		severalSet.SetActive(9)
		severalSet.SetActive(11)

		t.Run("several bits set (first)", func(t *testing.T) {
			i, err := severalSet.NextActive(9)
			assert.NoError(t, err)
			assert.Equal(t, uint32(9), i)
		})

		t.Run("several bits set (second)", func(t *testing.T) {
			i, err := severalSet.NextActive(10)
			assert.NoError(t, err)
			assert.Equal(t, uint32(11), i)
		})

		t.Run("several bits set (second, inclusive)", func(t *testing.T) {
			i, err := severalSet.NextActive(11)
			assert.NoError(t, err)
			assert.Equal(t, uint32(11), i)
		})
	})

	t.Run("many bytes", func(t *testing.T) {
		index := &CheckpointIndex{}
		index.SetActive(9)
		index.SetActive(16)
		index.SetActive(129)

		asked := []uint32{8, 9, 11, 129, 130}
		expected := []uint32{9, 9, 16, 129, 0}
		errors := []error{nil, nil, nil, nil, io.EOF}

		for i, chk := range asked {
			nextChk, err := index.NextActive(chk)
			assert.Equal(t, err, errors[i])
			assert.Equal(t, nextChk, expected[i])
		}
	})
}

func TestMaxBitAfter(t *testing.T) {
	for _, tc := range []struct {
		b     byte
		after uint32
		shift uint32
		ok    bool
	}{
		{0b0000_0000, 0, 0, false},
		{0b0000_0000, 1, 0, false},
		{0b1000_0000, 0, 0, true},
		{0b0100_0000, 0, 1, true},
		{0b0100_0000, 1, 1, true},
		{0b0010_1000, 0, 2, true},
		{0b0010_1000, 1, 2, true},
		{0b0010_1000, 2, 2, true},
		{0b0010_1000, 3, 4, true},
		{0b0010_1000, 4, 4, true},
		{0b0000_0001, 7, 7, true},
	} {
		t.Run(fmt.Sprintf("0b%b,%d", tc.b, tc.after), func(t *testing.T) {
			shift, ok := maxBitAfter(tc.b, tc.after)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.shift, shift)
		})
	}
}

func TestMerge(t *testing.T) {
	a := &CheckpointIndex{}
	require.NoError(t, a.SetActive(9))
	require.NoError(t, a.SetActive(129))

	b := &CheckpointIndex{}
	require.NoError(t, b.SetActive(900))
	require.NoError(t, b.SetActive(1000))

	var checkpoints []uint32
	b.iterate(func(c uint32) {
		checkpoints = append(checkpoints, c)
	})

	assert.Equal(t, []uint32{900, 1000}, checkpoints)

	require.NoError(t, a.Merge(b))

	assert.True(t, a.isActive(9))
	assert.True(t, a.isActive(129))
	assert.True(t, a.isActive(900))
	assert.True(t, a.isActive(1000))

	checkpoints = []uint32{}
	a.iterate(func(c uint32) {
		checkpoints = append(checkpoints, c)
	})

	assert.Equal(t, []uint32{9, 129, 900, 1000}, checkpoints)
}

func BenchmarkBitmapInsertion(b *testing.B) {
	checkpoints := makeCheckpointAccessPattern()
	b.ResetTimer()

	b.Run("Roaring", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bm := roaring.New()
			for _, chk := range checkpoints {
				bm.Add(chk)
			}
		}
	})

	b.Run("Custom", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bm := CheckpointIndex{}
			for _, chk := range checkpoints {
				bm.SetActive(chk)
			}
		}
	})
}
func BenchmarkBitmapNextActive(b *testing.B) {
	checkpoints := makeCheckpointAccessPattern()

	roar := roaring.New()
	paul := CheckpointIndex{}
	for _, chk := range checkpoints {
		roar.Add(chk)
		paul.SetActive(chk)
	}

	checkpointsToCheck := make([]uint32, 1000)
	for i, _ := range checkpointsToCheck {
		checkpointsToCheck[i] = uint32(1 + rand.Int31n(MAX_CHECKPOINTS))
	}

	roarResults := make([]int, len(checkpointsToCheck))
	paulResults := make([]int, len(checkpointsToCheck))

	b.ResetTimer()

	// The roaring bitmap doesn't have a "get next set bit" functionality, so we
	// have to use the iterator thingie they provide.
	b.Run("Roaring", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			for i, chk := range checkpointsToCheck {
				// iter := roar.Iterator()
				// iter.AdvanceIfNeeded(chk)
				// for iter.HasNext() {
				// 	value := iter.Next()
				// 	if value >= chk {
				// 		roarResults[i] = int(value)
				// 		break
				// 	}
				// }
				roarResults[i] = int(getNextActive(roar, chk))
			}
		}
	})

	b.Run("Custom", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			for i, chk := range checkpointsToCheck {
				value, _ := paul.NextActive(chk)
				paulResults[i] = int(value)
			}
		}
	})

	require.Equal(b, paulResults, roarResults)
	// "checking %v from %v", checkpointsToCheck, checkpoints)
}

func TestRoaringNextActive(t *testing.T) {
	chks := []uint32{3, 4, 10, 20, 30, 40}

	bm := roaring.BitmapOf(chks...)
	idx := &CheckpointIndex{}
	for _, chk := range chks {
		idx.SetActive(chk)
	}

	chk := []uint32{10, 11, 15, 19, 39, 40, 1, 50, 41, 31, 29}
	nextChk := []uint32{10, 20, 20, 20, 40, 40, 3, 0, 0, 40, 30}

	for i, chk := range chk {
		assert.Equalf(t, nextChk[i], getNextActive(bm, chk), "checkpoint: %d", chk)
		next, _ := idx.NextActive(chk)
		assert.Equalf(t, nextChk[i], next, "checkpoint: %d", chk)
	}
}

// getNextActive retrieves the next value stored in a bitmap after given value.
// If there is no such value, it returns 0.
func getNextActive(bm *roaring.Bitmap, needle uint32) uint32 {
	if min := bm.Minimum(); min >= needle {
		return min
	} else if needle > bm.Maximum() {
		return 0
	} else if bm.Contains(needle) {
		return needle
	}

	// We perform a fuzzy "nearest neighbor" binary search on the set of
	// available values in the bitmap. By fuzzy, we mean that `needle` is not
	// guaranteed to exist in the bitmap (in fact, it often won't), so we need
	// its next-largest neighbor. This means we need to check neighboring value
	// to see if they fit the "nearest" criteria.

	// Note: GetCardinality returns a uint64 but if there are more than 2**32
	// values in this bitmap idk what the heck is going on.
	low, high := uint32(0), uint32(bm.GetCardinality())
	for high > low {
		mid := (high + low) / 2

		value, err := bm.Select(mid)
		if err != nil {
			break
		}

		if value == needle {
			return value
		} else if value < needle {
			// we can use this moment to check if the next value is larger than
			// the needle
			nextValue, err := bm.Select(mid + 1)
			if err != nil {
				break // no next value means we'll never find it
			} else if nextValue >= needle {
				return nextValue
			}

			low = mid + 1
		} else { // implied: value > needle
			// if the previous value is smaller, this is the next-largest value
			prevValue, err := bm.Select(mid - 1)
			if err != nil || prevValue < needle {
				return value
			}

			high = mid - 1
		}
	}

	return 0
}

// Pubnet currently has ~41mln ledgers, so we can use this as an upper bound for
// benchmarking the size of a bitmap.
const MAX_CHECKPOINTS = 41_000_000 / 64

func makeCheckpointAccessPattern() []uint32 {
	// We shouldn't pick *random* ledgers to be active in, because this would
	// completely ruin the purpose of a bitmap. Something more reliable, then
	// would be to pick a random *range* and then activate most ledgers within
	// that range (but also at random).
	//
	// TODO: Actually follow my own advice above. ^

	// We suppose with absolutely no knowledge a prior or justification that 40%
	// "activity" within all checkpoints (and completely random checkpoints, at
	// that) is a reasonable metric.
	const ACTIVITY = int(MAX_CHECKPOINTS * 0.4)

	checkpoints := make([]uint32, ACTIVITY)
	for i, _ := range checkpoints {
		checkpoints[i] = uint32(1 + rand.Int31n(MAX_CHECKPOINTS))
	}

	// We don't want repeats (for performance and also fear of bugs..):
	keys := make(map[uint32]struct{})
	list := []uint32{}
	for _, entry := range checkpoints {
		if _, value := keys[entry]; !value {
			keys[entry] = struct{}{}
			list = append(list, entry)
		}
	}
	return list
}
