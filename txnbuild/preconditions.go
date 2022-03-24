package txnbuild

import (
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// Preconditions is a container for all transaction preconditions.
type Preconditions struct {
	// Transaction is only valid during a certain time range. This is private
	// because it should mirror the one set via TransactionParams, and this
	// association should be done via `NewPreconditions()`.
	Timebounds Timebounds
	// Transaction is valid for ledger numbers n such that minLedger <= n <
	// maxLedger (if maxLedger == 0, then only minLedger is checked)
	Ledgerbounds *Ledgerbounds
	// If nil, the transaction is only valid when sourceAccount's sequence
	// number "N" is seqNum - 1. Otherwise, valid when N satisfies minSeqNum <=
	// N < tx.seqNum.
	MinSequenceNumber *int64
	// Transaction is valid if the current ledger time is at least
	// minSequenceNumberAge greater than the source account's seqTime.
	MinSequenceNumberAge xdr.Duration
	// Transaction is valid if the current ledger number is at least
	// minSequenceNumberLedgerGap greater than the source account's seqLedger.
	MinSequenceNumberLedgerGap uint32
	// Transaction is valid if there is a signature corresponding to every
	// Signer in this array, even if the signature is not otherwise required by
	// the source account or operations.
	ExtraSigners []xdr.SignerKey
}

// NewPreconditions creates a set of preconditions with timebounds enabled
func NewPreconditions(timebounds Timebounds) Preconditions {
	cond := Preconditions{Timebounds: timebounds}
	return cond
}

func NewPreconditionsWithTimebounds(minTime, maxTime int64) Preconditions {
	return NewPreconditions(NewTimebounds(minTime, maxTime))
}

// Validate ensures that all enabled preconditions are valid.
func (cond *Preconditions) Validate() error {
	var err error

	if err = cond.Timebounds.Validate(); err != nil {
		return err
	}

	if ok := cond.ValidateSigners(); !ok {
		return errors.New("invalid signers")
	}

	if cond.Ledgerbounds != nil {
		err = cond.Ledgerbounds.Validate()
		if err != nil {
			return err
		}
	}

	return nil
}

func (cond *Preconditions) ValidateSigners() bool {
	return len(cond.ExtraSigners) <= 2
}

// BuildXDR will create a precondition structure that varies depending on
// whether or not there are additional preconditions besides timebounds (which
// are required).
func (cond *Preconditions) BuildXDR() xdr.Preconditions {
	xdrCond := xdr.Preconditions{}
	xdrTimeBounds := xdr.TimeBounds{
		MinTime: xdr.TimePoint(cond.Timebounds.MinTime),
		MaxTime: xdr.TimePoint(cond.Timebounds.MaxTime),
	}

	// Only build PRECOND_V2 structure if we need to
	if cond.hasV2Conditions() {
		xdrPrecond := xdr.PreconditionsV2{
			TimeBounds:      &xdrTimeBounds,
			MinSeqAge:       cond.MinSequenceNumberAge,
			MinSeqLedgerGap: xdr.Uint32(cond.MinSequenceNumberLedgerGap),
			ExtraSigners:    cond.ExtraSigners,
		}

		// micro-optimization: if the ledgerbounds will always succeed, omit them
		if cond.Ledgerbounds != nil && !(cond.Ledgerbounds.MinLedger == 0 &&
			cond.Ledgerbounds.MaxLedger == 0) {
			xdrPrecond.LedgerBounds = &xdr.LedgerBounds{
				MinLedger: xdr.Uint32(cond.Ledgerbounds.MinLedger),
				MaxLedger: xdr.Uint32(cond.Ledgerbounds.MaxLedger),
			}
		}

		if cond.MinSequenceNumber != nil {
			seqNum := xdr.SequenceNumber(*cond.MinSequenceNumber)
			xdrPrecond.MinSeqNum = &seqNum
		}

		xdrCond.Type = xdr.PreconditionTypePrecondV2
		xdrCond.V2 = &xdrPrecond
	} else {
		xdrCond.Type = xdr.PreconditionTypePrecondTime
		xdrCond.TimeBounds = &xdrTimeBounds
	}

	return xdrCond
}

// hasV2Conditions determines whether or not this has conditions on top of
// the (required) timebound precondition.
func (cond *Preconditions) hasV2Conditions() bool {
	return (cond.Ledgerbounds != nil ||
		cond.MinSequenceNumber != nil ||
		cond.MinSequenceNumberAge > xdr.Duration(0) ||
		cond.MinSequenceNumberLedgerGap > 0 ||
		len(cond.ExtraSigners) > 0)
}
