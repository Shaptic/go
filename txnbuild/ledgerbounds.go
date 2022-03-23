package txnbuild

import "github.com/stellar/go/support/errors"

// Ledgerbounds represent a transaction precondition that controls the ledger
// range for which a transaction is valid. Setting MaxLedger = 0 indicates there
// is no maximum ledger.
type Ledgerbounds struct {
	MinLedger uint32
	MaxLedger uint32
}

func (lb *Ledgerbounds) Validate() error {
	if lb.MaxLedger > 0 && lb.MaxLedger < lb.MinLedger {
		return errors.New("invalid ledgerbound: max ledger < min ledger")
	}

	return nil
}
