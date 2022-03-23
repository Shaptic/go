package xdr_test

import (
	"fmt"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestDecoratedSignatures(t *testing.T) {
	signature := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}
	keyHint := [4]byte{9, 10, 11, 12}

	testCases := []struct {
		payload      []byte
		expectedHint [4]byte
	}{
		{
			payload:      []byte{13, 14, 15, 16, 17, 18, 19, 20, 21},
			expectedHint: [4]byte{27, 25, 31, 25},
		},
		{
			payload:      []byte{18, 19, 20},
			expectedHint: [4]byte{27, 25, 31, 12},
		},
	}

	for _, testCase := range testCases {
		t.Run(
			fmt.Sprintf("%d-byte signed payload decorated sig", len(testCase.payload)),
			func(t *testing.T) {
				decoSig := xdr.NewDecoratedSignature(signature, keyHint)
				assert.EqualValues(t, keyHint, decoSig.Hint)
				assert.EqualValues(t, signature, decoSig.Signature)
			})

		t.Run(
			fmt.Sprintf("%d-byte decorated sig", len(testCase.payload)),
			func(t *testing.T) {
				decoSig := xdr.NewDecoratedSignatureForPayload(
					signature, keyHint, testCase.payload)
				assert.EqualValues(t, testCase.expectedHint, decoSig.Hint)
				assert.EqualValues(t, signature, decoSig.Signature)
			})
	}
}
