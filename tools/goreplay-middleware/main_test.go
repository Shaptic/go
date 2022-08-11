package main

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcess(t *testing.T) {
	// For 1 type, it returns the same msg to stdout
	payload := "1 ID\nGET /ledgers HTTP/1.1\nHost: horizon.stellar.org\n\n"
	stdin := strings.NewReader(hex.EncodeToString([]byte(payload)))

	var stdout, stderr bytes.Buffer
	processAll(stdin, &stderr, &stdout)

	decodedOut, err := hex.DecodeString(stdout.String())
	assert.NoError(t, err)
	assert.Equal(t, payload, string(decodedOut))
	assert.Equal(t, "", stderr.String())
}
