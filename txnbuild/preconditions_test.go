package txnbuild

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

var Signers = []xdr.SignerKey{
	xdr.MustSigner("GAOQJGUAB7NI7K7I62ORBXMN3J4SSWQUQ7FOEPSDJ322W2HMCNWPHXFB"),
	xdr.MustSigner("GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ"),
	xdr.MustSigner("PA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUAAAAAQACAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DUPB6IBZGM"),
}

var (
	seqNum    = int64(14)
	xdrSeqNum = xdr.SequenceNumber(seqNum)
	xdrCond   = xdr.Preconditions{
		Type: xdr.PreconditionTypePrecondV2,
		V2: &xdr.PreconditionsV2{
			TimeBounds: &xdr.TimeBounds{
				MinTime: xdr.TimePoint(27),
				MaxTime: xdr.TimePoint(42),
			},
			LedgerBounds: &xdr.LedgerBounds{
				MinLedger: xdr.Uint32(27),
				MaxLedger: xdr.Uint32(42),
			},
			MinSeqNum:       &xdrSeqNum,
			MinSeqAge:       xdr.Duration(27),
			MinSeqLedgerGap: xdr.Uint32(42),
			ExtraSigners:    Signers[:1],
		},
	}
	tb = NewTimebounds(27, 42)
	pc = Preconditions{
		timebounds:                 &tb,
		Ledgerbounds:               &Ledgerbounds{27, 42},
		MinSequenceNumber:          &seqNum,
		MinSequenceNumberAge:       xdr.Duration(27),
		MinSequenceNumberLedgerGap: 42,
		ExtraSigners:               Signers[:1],
	}
)

// TestClassifyingPreconditions ensures that Preconditions will correctly
// differentiate V1 (timebounds-only) or V2 (all other) preconditions correctly.
func TestClassifyingPreconditions(t *testing.T) {
	tbpc := NewPreconditions(&tb)
	assert.False(t, (&Preconditions{}).HasV2Conditions())
	assert.False(t, tbpc.HasV2Conditions())
	assert.True(t, pc.HasV2Conditions())
}

// TestPreconditions ensures correct XDR is generated for a (non-exhaustive)
// handful of precondition combinations.
func TestPreconditions(t *testing.T) {
	preconditionModifiers := []struct {
		Name     string
		Modifier func() (xdr.Preconditions, Preconditions)
	}{
		{
			"unchanged",
			func() (xdr.Preconditions, Preconditions) {
				return xdrCond, pc
			},
		},
		{
			"only timebounds",
			func() (xdr.Preconditions, Preconditions) {
				tb := NewTimebounds(1, 2)
				return xdr.Preconditions{
					Type: xdr.PreconditionTypePrecondTime,
					TimeBounds: &xdr.TimeBounds{
						MinTime: xdr.TimePoint(1),
						MaxTime: xdr.TimePoint(2),
					},
				}, NewPreconditions(&tb)
			},
		},
		{
			"unbounded ledgerbounds",
			func() (xdr.Preconditions, Preconditions) {
				newCond, newPc := clone(xdrCond, pc)
				newCond.V2.LedgerBounds.MaxLedger = 0
				newPc.Ledgerbounds.MaxLedger = 0
				return newCond, newPc
			},
		},
		{
			"nil ledgerbounds",
			func() (xdr.Preconditions, Preconditions) {
				newCond, newPc := clone(xdrCond, pc)
				newCond.V2.LedgerBounds = nil
				newPc.Ledgerbounds = nil
				return newCond, newPc
			},
		},
		{
			"nil minSeq",
			func() (xdr.Preconditions, Preconditions) {
				newCond, newPc := clone(xdrCond, pc)
				newCond.V2.MinSeqNum = nil
				newPc.MinSequenceNumber = nil
				return newCond, newPc
			},
		},
	}
	for _, testCase := range preconditionModifiers {
		t.Run(testCase.Name, func(t *testing.T) {
			xdrCond, precond := testCase.Modifier()
			assert.NoError(t, precond.Validate())

			expectedBytes, err := xdrCond.MarshalBinary()
			assert.NoError(t, err)

			actualBytes, err := precond.BuildXDR().MarshalBinary()
			assert.NoError(t, err)
			assert.Equal(t, expectedBytes, actualBytes)
		})
	}
}

// TestPreconditionsValidation ensures that validation fails when necessary.
func TestPreconditionsValidation(t *testing.T) {
	t.Run("too many signers", func(t *testing.T) {
		pc.ExtraSigners = Signers
		assert.Error(t, pc.Validate())
	})
}

func clone(pcXdr xdr.Preconditions, pc Preconditions) (xdr.Preconditions, Preconditions) {
	return cloneXdrPreconditions(pcXdr), clonePreconditions(pc)
}

func cloneXdrPreconditions(pc xdr.Preconditions) xdr.Preconditions {
	binary, err := pc.MarshalBinary()
	if err != nil {
		panic(err)
	}

	clone := xdr.Preconditions{}
	if err = clone.UnmarshalBinary(binary); err != nil {
		panic(err)
	}

	return clone
}

func clonePreconditions(precond Preconditions) Preconditions {
	tb := NewTimebounds(precond.timebounds.MinTime, precond.timebounds.MaxTime)
	cond := NewPreconditions(&tb)
	if precond.Ledgerbounds != nil {
		cond.Ledgerbounds = &Ledgerbounds{
			MinLedger: precond.Ledgerbounds.MinLedger,
			MaxLedger: precond.Ledgerbounds.MaxLedger,
		}
	}

	if precond.MinSequenceNumber != nil {
		cond.MinSequenceNumber = precond.MinSequenceNumber
	}

	cond.MinSequenceNumberAge = precond.MinSequenceNumberAge
	cond.MinSequenceNumberLedgerGap = precond.MinSequenceNumberLedgerGap

	if len(precond.ExtraSigners) > 0 {
		cond.ExtraSigners = append(cond.ExtraSigners, precond.ExtraSigners...)
	}

	return cond
}
