//lint:file-ignore U1001 Ignore all unused code, staticcheck doesn't understand testify/suite
package txnbuild

import (
	"github.com/stellar/go/amount"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// LiquidityPoolWithdraw represents the Stellar liquidity pool withdraw operation. See
// https://www.stellar.org/developers/guides/concepts/list-of-operations.html
type LiquidityPoolWithdraw struct {
	SourceAccount   string
	LiquidityPoolID LiquidityPoolId
	Amount          string
	MinAmountA      string
	MinAmountB      string
}

// BuildXDR for LiquidityPoolWithdraw returns a fully configured XDR Operation.
func (lpd *LiquidityPoolWithdraw) BuildXDR(withMuxedAccounts bool) (xdr.Operation, error) {
	xdrLiquidityPoolId, err := lpd.LiquidityPoolID.ToXDR()
	if err != nil {
		return xdr.Operation{}, errors.Wrap(err, "couldn't build liquidity pool ID XDR")
	}

	xdrAmount, err := amount.Parse(lpd.Amount)
	if err != nil {
		return xdr.Operation{}, errors.Wrap(err, "failed to parse 'Amount'")
	}

	xdrMinAmountA, err := amount.Parse(lpd.MinAmountA)
	if err != nil {
		return xdr.Operation{}, errors.Wrap(err, "failed to parse 'MinAmountA'")
	}

	xdrMinAmountB, err := amount.Parse(lpd.MinAmountB)
	if err != nil {
		return xdr.Operation{}, errors.Wrap(err, "failed to parse 'MinAmountB'")
	}

	xdrOp := xdr.LiquidityPoolWithdrawOp{
		LiquidityPoolId: xdrLiquidityPoolId,
		Amount:          xdrAmount,
		MinAmountA:      xdrMinAmountA,
		MinAmountB:      xdrMinAmountB,
	}

	opType := xdr.OperationTypeLiquidityPoolWithdraw
	body, err := xdr.NewOperationBody(opType, xdrOp)
	if err != nil {
		return xdr.Operation{}, errors.Wrap(err, "failed to build XDR OperationBody")
	}
	op := xdr.Operation{Body: body}
	if withMuxedAccounts {
		SetOpSourceMuxedAccount(&op, lpd.SourceAccount)
	} else {
		SetOpSourceAccount(&op, lpd.SourceAccount)
	}
	return op, nil
}

// FromXDR for LiquidityPoolWithdraw initializes the txnbuild struct from the corresponding xdr Operation.
func (lpd *LiquidityPoolWithdraw) FromXDR(xdrOp xdr.Operation, withMuxedAccounts bool) error {
	result, ok := xdrOp.Body.GetLiquidityPoolWithdrawOp()
	if !ok {
		return errors.New("error parsing liquidity_pool_withdraw operation from xdr")
	}

	liquidityPoolID, err := liquidityPoolIdFromXDR(result.LiquidityPoolId)
	if err != nil {
		return errors.New("error parsing LiquidityPoolId in liquidity_pool_withdraw operation from xdr")
	}
	lpd.LiquidityPoolID = liquidityPoolID

	lpd.SourceAccount = accountFromXDR(xdrOp.SourceAccount, withMuxedAccounts)
	lpd.Amount = amount.String(result.Amount)
	lpd.MinAmountA = amount.String(result.MinAmountA)
	lpd.MinAmountB = amount.String(result.MinAmountB)

	return nil
}

// Validate for LiquidityPoolWithdraw validates the required struct fields. It returns an error if any of the fields are
// invalid. Otherwise, it returns nil.
func (lpd *LiquidityPoolWithdraw) Validate(withMuxedAccounts bool) error {
	err := validateAmount(lpd.Amount)
	if err != nil {
		return NewValidationError("Amount", err.Error())
	}

	err = validateAmount(lpd.MinAmountA)
	if err != nil {
		return NewValidationError("MinAmountA", err.Error())
	}

	err = validateAmount(lpd.MinAmountB)
	if err != nil {
		return NewValidationError("MinAmountB", err.Error())
	}

	return nil

}

// GetSourceAccount returns the source account of the operation, or nil if not
// set.
func (lpd *LiquidityPoolWithdraw) GetSourceAccount() string {
	return lpd.SourceAccount
}