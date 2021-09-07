package orderbook

import (
	"fmt"
	"math"
	"math/big"
	"math/bits"
)

// Uint128 defines an unsigned, 128-bit integer.
//
// Since Golang does not have a built-in uint128 type, we need to implement some
// of these operations ourselves to get high-performance math.
//
// Unfortunately, we do still need to leverage Go's math/big.Int in some places
// where math/bits is unwieldy, but we do get the performance bump wherever that
// module isn't necessary.
type Uint128 struct {
	Hi uint64
	Lo uint64
}

var Zero = NewUint128(0, 0)

func NewUint128(hi, lo uint64) *Uint128 {
	return &Uint128{Hi: hi, Lo: lo}
}

func (i *Uint128) IsUint64() bool {
	return i.Hi == 0
}

func (i *Uint128) Uint64() uint64 {
	return i.Lo
}

func (i *Uint128) Less(n *Uint128) bool {
	return i.Hi < n.Hi || (i.Hi == n.Hi && i.Lo < n.Lo)
}

func (i *Uint128) Equals(n *Uint128) bool {
	return i.Hi == n.Hi && i.Lo == n.Lo
}

func (i *Uint128) ToBigInt() *big.Int {
	A := new(big.Int).SetUint64(i.Hi)
	B := new(big.Int).SetUint64(i.Lo)
	return A.Lsh(A, 32).Add(A, B)
}

func (i *Uint128) String() string {
	return fmt.Sprintf("%d|%d", i.Hi, i.Lo)
}

// BigAdd combines two uint64s
func BigAdd(a, b uint64) *Uint128 {
	r, carry := bits.Add64(a, b, 0)
	return NewUint128(carry, r)
}

// HugeAdd combines two uint128s, returning false on overflow
func HugeAdd(a, b *Uint128) (*Uint128, bool) {
	rLo, carry := bits.Add64(a.Lo, b.Lo, 0)
	rHi, carry := bits.Add64(a.Hi, b.Hi, carry)
	return NewUint128(rHi, rLo), carry == 0
}

// BigMul multiplies two uint64s
func BigMul(a, b uint64) *Uint128 {
	rHi, rLo := bits.Mul64(a, b)
	return NewUint128(rHi, rLo)
}

// HugeMul multiplies two uint128s, returning false on overflow.
// It uses big.Int internally, so it's not high-performance.
func HugeMul(a, b *Uint128) (*Uint128, bool) {
	// We need to support 128-bit multiplies, but this is hard to do right, so
	// we have to leverage math/big here.
	//
	// We can improve performance by:
	//
	//  - implementing https://stackoverflow.com/a/31662911
	//  - lean on https://github.com/davidminor/uint128/blob/master/uint128.go#L69-L94
	//  - converting the assembly: https://godbolt.org/g/8YiZXX.
	//
	// More discussion here on adding native 128-bit integer support is here:
	// https://github.com/golang/go/issues/9455
	A, B := a.ToBigInt(), b.ToBigInt()

	if r := BigToUint128(A.Mul(A, B)); r != nil {
		return r, true
	}

	return Zero, false
}

// BigDiv divides a uint128 by a uint64, returning the quotient, remainder, and
// false if the division would overflow or panic.
func BigDiv(a *Uint128, b uint64) (*Uint128, uint64, bool) {
	if b == 0 || b <= a.Hi {
		return Zero, 0, false
	}

	q, r := bits.Div64(a.Hi, a.Lo, b)
	return NewUint128(0, q), r, true
}

// HugeDiv divides two uint128s, returning the quotient and remainder.
// It uses big.Int internally, so it's not high-performance, and it will panic
// on a divide-by-zero.
func HugeDiv(a, b *Uint128) (*Uint128, *Uint128) {
	// Even though we don't care about results where the quotient is >=
	// MaxUint64, we cannot preemtively determine this. Thus, we once again need
	// to leverage math/big.
	A, B := a.ToBigInt(), b.ToBigInt()

	R := big.NewInt(0)
	A.DivMod(A, B, R)

	return BigToUint128(A), BigToUint128(R)
}

// BigToUint128 converts a big.Int to its uint128 equivalent, returning nil if
// it's too big to fit.
func BigToUint128(i *big.Int) *Uint128 {
	if i.BitLen() > 128 {
		return nil
	}

	// Turn i into a Uint128 by collecting the two halves.
	andHi := new(big.Int).SetUint64(0x00000000FFFFFFFF)
	rHi := big.NewInt(0).Rsh(i, 64)    // get top 64 bits as uint64
	rLo := big.NewInt(0).And(i, andHi) // drop top bits to get bottom ones
	return NewUint128(rHi.Uint64(), rLo.Uint64())
}

// MulThenDiv returns a*b/c where a*b would've overflowed a uint64, and false if
// the result still overflows a uint64 (or parameters are invalid).
func MulThenDiv(a uint32, b, c *Uint128) (uint64, bool) {
	if a <= 0 {
		return 0, false
	}

	if c.Equals(Zero) {
		return 0, false
	}

	// would overflow no matter what
	if !c.Less(BigMul(math.MaxInt32, math.MaxInt64)) {
		return 0, false
	}

	Q, R := HugeDiv(b, c)
	if !Q.IsUint64() {
		return 0, false
	}

	// We can evaluate A * Q because
	//     A * Q <= INT32_MAX * INT64_MAX
	//           <  INT128_MAX
	// We can evaluate A * R because
	//     A * R < A * C
	//           < INT32_MAX * (INT32_MAX * INT64_MAX)
	//           < INT128_MAX
	//
	// Combining these results, we can evaluate
	//     A * Q + A * R / C < 2 * INT128_MAX < UINT128_MAX
	//
	// The result is (A * Q) + (A * R / C)
	A := NewUint128(0, uint64(a))

	AtimesQ := BigMul(uint64(a), Q.Uint64())
	AtimesR, _ := HugeMul(A, R)                   // no overflow: invariant
	AtimesRdivC, _ := HugeDiv(AtimesR, c)         // already checked for div-by-0
	bigResult, _ := HugeAdd(AtimesQ, AtimesRdivC) // no overflow: invariant

	return bigResult.Uint64(), bigResult.IsUint64()
}
