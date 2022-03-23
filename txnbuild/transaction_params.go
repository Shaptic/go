package txnbuild

import (
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// TransactionParams is a container for parameters which are used to construct
// new Transaction instances
type TransactionParams struct {
	SourceAccount        Account
	IncrementSequenceNum bool
	Operations           []Operation
	BaseFee              int64
	Memo                 Memo

	// Transaction validity preconditions
	Timebounds                 Timebounds
	Ledgerbounds               *Ledgerbounds
	MinSequenceNumber          *int64
	MinSequenceNumberAge       xdr.Duration
	MinSequenceNumberLedgerGap uint32
	ExtraSigners               []xdr.SignerKey
}

// Validate ensures that all enabled preconditions are valid.
func (params *TransactionParams) Validate() error {
	var err error

	if err = params.Timebounds.Validate(); err != nil {
		return err
	}

	if len(params.ExtraSigners) > 2 {
		return errors.New("too many signers")
	}

	if params.Ledgerbounds != nil {
		err = params.Ledgerbounds.Validate()
		if err != nil {
			return err
		}
	}

	return nil
}

// hasV2Conditions determines whether or not this has conditions on top of
// the (required) timebound precondition.
func (params *TransactionParams) hasV2Conditions() bool {
	return (params.Ledgerbounds != nil ||
		params.MinSequenceNumber != nil ||
		params.MinSequenceNumberAge > xdr.Duration(0) ||
		params.MinSequenceNumberLedgerGap > 0 ||
		len(params.ExtraSigners) > 0)
}

// BuildPreconditionsXDR will create a precondition structure that varies
// depending on whether or not there are additional preconditions besides
// timebounds (which are required).
func (params *TransactionParams) BuildPreconditionsXDR() xdr.Preconditions {
	xdrCond := xdr.Preconditions{}
	xdrTimeBounds := xdr.TimeBounds{
		MinTime: xdr.TimePoint(params.Timebounds.MinTime),
		MaxTime: xdr.TimePoint(params.Timebounds.MaxTime),
	}

	// Only build the PRECOND_V2 structure if we need to
	if params.hasV2Conditions() {
		xdrPrecond := xdr.PreconditionsV2{
			TimeBounds:      &xdrTimeBounds,
			MinSeqAge:       params.MinSequenceNumberAge,
			MinSeqLedgerGap: xdr.Uint32(params.MinSequenceNumberLedgerGap),
			ExtraSigners:    params.ExtraSigners,
		}

		// micro-optimization: if the ledgerbounds will always succeed, omit them
		if params.Ledgerbounds != nil && !(params.Ledgerbounds.MinLedger == 0 &&
			params.Ledgerbounds.MaxLedger == 0) {
			xdrPrecond.LedgerBounds = &xdr.LedgerBounds{
				MinLedger: xdr.Uint32(params.Ledgerbounds.MinLedger),
				MaxLedger: xdr.Uint32(params.Ledgerbounds.MaxLedger),
			}
		}

		if params.MinSequenceNumber != nil {
			seqNum := xdr.SequenceNumber(*params.MinSequenceNumber)
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
