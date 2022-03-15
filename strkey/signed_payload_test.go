package strkey

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSignedPayloads(t *testing.T) {
	testCases := []struct {
		hexPayload string
		signer     string
		expected   string
	}{
		// Valid signed payload with an ed25519 public key and a 32-byte
		// payload.
		{
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			"GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ",
			"PA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUAAAAAQACAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DUPB6IBZGM",
		},
		// Valid signed payload with an ed25519 public key and a 29-byte payload
		// which becomes zero padded.
		{
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d",
			"GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ",
			"PA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUAAAAAOQCAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DUAAAAFGBU",
		},
	}

	for _, testCase := range testCases {
		payload, _ := hex.DecodeString(testCase.hexPayload)
		sp, err := MakeSignedPayload(testCase.signer, payload)
		if !assert.NoError(t, err) || !assert.NotNil(t, sp) {
			t.Fail()
			continue
		}

		actual, err := sp.Encode()
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, actual)

		sp, err = DecodeSignedPayload(testCase.expected)
		if !assert.NoError(t, err) || !assert.NotNil(t, sp) {
			t.Fail()
			continue
		}
		assert.Equal(t, testCase.signer, sp.Signer)
		assert.Equal(t, testCase.hexPayload, hex.EncodeToString(sp.Payload))
	}
}

func TestSignedPayloadErrors(t *testing.T) {
	testCases := []string{
		// Length prefix specifies length that is shorter than payload in signed payload
		"PA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUAAAAAQACAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DUPB6IAAAAAAAAPM",
		// Length prefix specifies length that is longer than payload in signed payload
		"PA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUAAAAAOQCAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4Z2PQ",
		// No zero padding in signed payload
		"PA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUAAAAAOQCAQDAQCQMBYIBEFAWDANBYHRAEISCMKBKFQXDAMRUGY4DXFH6",
	}

	for _, testCase := range testCases {
		sp, err := DecodeSignedPayload(testCase)
		assert.Error(t, err)
		assert.Nil(t, sp)
	}
}

// TestSignedPayloadSizes ensures all valid payload lengths work
func TestSignedPayloadSizes(t *testing.T) {
	signer := "GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ"

	for length := 0; length <= 64; length++ {
		payload := make([]byte, length)
		_, err := rand.Read(payload)
		assert.NoError(t, err)

		sp, err := MakeSignedPayload(signer, payload)
		if !assert.NotNil(t, sp) || !assert.NoError(t, err) {
			t.Fail()
			continue
		}
		assert.Equal(t, signer, sp.Signer)
		assert.True(t, bytes.Equal(payload, sp.Payload))

		_, err = sp.Encode()
		assert.NoError(t, err)
	}

	for _, length := range []int{65} {
		payload := make([]byte, length)
		_, err := rand.Read(payload)
		assert.NoError(t, err)

		sp, err := MakeSignedPayload(signer, payload)
		assert.Nil(t, sp)
		assert.Error(t, err)
	}
}
