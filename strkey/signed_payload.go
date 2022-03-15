package strkey

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/stellar/go/support/errors"
)

type SignedPayload struct {
	Signer  string
	Payload []byte
}

func MakeSignedPayload(signerPublicKey string, payload []byte) (*SignedPayload, error) {
	if len(payload) > 64 {
		return nil, errors.Errorf("expected <= 64-byte payload, got %d", len(payload))
	}

	src := make([]byte, len(payload))
	copy(src, payload)
	return &SignedPayload{Signer: signerPublicKey, Payload: src}, nil
}

// Encode turns a signed payload structure into its StrKey equivalent.
func (sp *SignedPayload) Encode() (string, error) {
	signerBytes, err := Decode(VersionByteAccountID, sp.Signer)
	if err != nil {
		return "", errors.Wrap(err, "failed to encode signed payload")
	}

	// Encoding has four parts:
	//  - 32-byte signer public key
	//  - 4-byte payload length
	//  - N-byte payload (where N <= 64)
	//  - M-byte padding (where 0 <= M <= 4)
	baseLength := 32 + 4 + len(sp.Payload)
	padding := getNextMultiple(baseLength, 4) - baseLength
	src := make([]byte, baseLength+padding)

	copy(src[:32], signerBytes)                                     // signer
	binary.BigEndian.PutUint32(src[32:36], uint32(len(sp.Payload))) // length
	copy(src[36:], sp.Payload)                                      // payload
	for i := 0; i < padding; i++ {                                  // padding
		src[36+len(sp.Payload)+i] = byte(0)
	}

	strkey, err := Encode(VersionByteSignedPayload, src[:])
	if err != nil {
		return "", err
	}
	return strkey, nil
}

func DecodeSignedPayload(address string) (*SignedPayload, error) {
	raw, err := Decode(VersionByteSignedPayload, address)
	if err != nil {
		return nil, errors.New("invalid signed payload")
	}
	if len(raw) > 32+4+64 {
		return nil, errors.Errorf("signed payload too large: %d", len(raw))
	}
	if len(raw)%4 != 0 {
		return nil, errors.Errorf("signed payload has invalid padding")
	}

	signer, err := Encode(VersionByteAccountID, raw[:32])
	if err != nil {
		return nil, errors.Wrap(err, "signed payload has invalid signer")
	}

	var payloadLen uint32 = 0
	reader := bytes.NewBuffer(raw[32:])
	err = binary.Read(reader, binary.BigEndian, &payloadLen)
	if err != nil {
		return nil, errors.Wrap(err,
			fmt.Sprintf("signed payload has invalid length"))
	}

	if payloadLen > 64 {
		return nil, fmt.Errorf("signed payload has invalid length: %d", payloadLen)
	}

	if getNextMultiple(int(payloadLen), 4) != len(raw[36:]) {
		return nil, fmt.Errorf("signed payload has invalid padding")
	}

	return MakeSignedPayload(signer, raw[36:36+payloadLen])
}

func getNextMultiple(x, n int) int {
	return int(math.Ceil(float64(x)/float64(n)) * float64(n))
}
