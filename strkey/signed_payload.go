package strkey

import (
	"bytes"
	"math"

	xdr "github.com/stellar/go-xdr/xdr3"
	"github.com/stellar/go/support/errors"
)

type SignedPayload struct {
	Signer  string
	Payload []byte
}

const maxPayloadLen = 64

func MakeSignedPayload(signerPublicKey string, payload []byte) (*SignedPayload, error) {
	if len(payload) > maxPayloadLen {
		return nil, errors.Errorf("payload length %d exceeds max %d",
			len(payload), maxPayloadLen)
	}

	src := make([]byte, len(payload))
	copy(src, payload)
	return &SignedPayload{Signer: signerPublicKey, Payload: src}, nil
}

// Encode turns a signed payload structure into its StrKey equivalent.
func (sp *SignedPayload) Encode() (string, error) {
	signerBytes, err := Decode(VersionByteAccountID, sp.Signer)
	if err != nil {
		return "", errors.Wrap(err, "failed to decode signed payload signer")
	}

	b := new(bytes.Buffer)
	b.Write(signerBytes)
	xdr.Marshal(b, sp.Payload)

	strkey, err := Encode(VersionByteSignedPayload, b.Bytes())
	if err != nil {
		return "", errors.Wrap(err, "failed to encode signed payload")
	}
	return strkey, nil
}

func DecodeSignedPayload(address string) (*SignedPayload, error) {
	raw, err := Decode(VersionByteSignedPayload, address)
	if err != nil {
		return nil, errors.New("invalid signed payload")
	}

	const signerLen = 32
	rawSigner, raw := raw[:signerLen], raw[signerLen:]
	signer, err := Encode(VersionByteAccountID, rawSigner)
	if err != nil {
		return nil, errors.Wrap(err, "invalid signed payload signer")
	}

	payload := []byte{}
	reader := bytes.NewBuffer(raw)
	readBytes, err := xdr.Unmarshal(reader, &payload)
	if err != nil {
		return nil, errors.Wrap(err, "invalid signed payload")
	}

	if len(raw) != readBytes || reader.Len() > 0 {
		return nil, errors.New("invalid signed payload padding")
	}

	return MakeSignedPayload(signer, payload)
}

func getNextMultiple(x, n int) int {
	return int(math.Ceil(float64(x)/float64(n)) * float64(n))
}
