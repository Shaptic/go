package xdr

// NewDecoratedSignature constructs a decorated signature structure directly
// from the given signature and key hint. Note that the key hint should
// correspond to the key that created the signature, but this helper cannot
// ensure that.
func NewDecoratedSignature(sig []byte, keyHint [4]byte) DecoratedSignature {
	return DecoratedSignature{
		Hint:      SignatureHint(keyHint),
		Signature: Signature(sig),
	}
}

// NewDecoratedSignatureForPayload creates a decorated signature with a hint
// that uses the key hint, the last four bytes of signature, and the last four
// bytes of the input that got signed. Note that the signature should be the
// signature of the payload via the key being hinted, but this construction
// method cannot ensure that.
func NewDecoratedSignatureForPayload(
	sig []byte, keyHint [4]byte, payload []byte,
) DecoratedSignature {
	hint := [4]byte{}
	j := iMax(0, len(payload)-4)

	for i := 0; i < len(keyHint); i++ {
		hint[i] = keyHint[i]
		if j < len(payload) {
			hint[i] ^= payload[j]
			j++
		}
	}

	return DecoratedSignature{
		Hint:      SignatureHint(hint),
		Signature: Signature(sig),
	}
}

func iMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
