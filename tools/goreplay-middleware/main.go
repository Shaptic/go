// Based on https://github.com/buger/goreplay/blob/master/examples/middleware/token_modifier.go
package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"
)

// maxPerSecond defines how many requests should be checked at max per second
const maxPerSecond = 1

const (
	requestType          byte = '1'
	originalResponseType byte = '2'
	replayedResponseType byte = '3'
)

var lastCheck = time.Now()
var reqsCheckedPerSeq = 0
var pendingRequests map[string]*Request

func main() {
	processAll(os.Stdin, os.Stderr, os.Stdout)
}

func processAll(stdin io.Reader, stderr, stdout io.StringWriter) {
	scanner := bufio.NewScanner(stdin)
	pendingRequests = make(map[string]*Request)

	for scanner.Scan() {
		encoded := scanner.Bytes()
		buf := make([]byte, len(encoded)/2)
		_, err := hex.Decode(buf, encoded)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("hex.Decode error: %v", err))
			continue
		}

		process(stderr, stdout, buf)
	}
}

func process(stderr, stdout io.StringWriter, buf []byte) {
	// First byte indicate payload type:
	payloadType := buf[0]
	headerSize := bytes.IndexByte(buf, '\n') + 1
	header := buf[:headerSize-1]

	// Header contains space separated values of: request type, request id, and request start time (or round-trip time for responses)
	meta := bytes.Split(header, []byte(" "))
	// For each request you should receive 3 payloads (request, response, replayed response) with same request id
	reqID := string(meta[1])
	payload := buf[headerSize:]

	// debug
	os.Stderr.WriteString(fmt.Sprintf("%c %s\n", payloadType, reqID))

	switch payloadType {
	case requestType:
		if time.Since(lastCheck) > time.Second {
			reqsCheckedPerSeq = 0
			lastCheck = time.Now()
		}

		if reqsCheckedPerSeq < maxPerSecond {
			pendingRequests[reqID] = &Request{
				Headers: string(buf),
			}
			reqsCheckedPerSeq++
		}

		// Emitting data back, without modification
		_, err := stdout.WriteString(hex.EncodeToString(buf))
		if err != nil {
			stderr.WriteString(fmt.Sprintf("stdout.WriteString error: %v", err))
		}
	case originalResponseType:
		if req, ok := pendingRequests[reqID]; ok {
			// Token is inside response body
			req.OriginalResponse = payload
		}
	case replayedResponseType:
		if req, ok := pendingRequests[reqID]; ok {
			delete(pendingRequests, reqID)
			req.MirroredResponse = payload

			if !req.ResponseEquals() {
				// TODO improve the message to at least print the requested path
				// TODO in the future publish the results to S3 for easier processing
				stderr.WriteString("MISMATCH " + req.SerializeBase64() + "\n")
			}
		}
	}
}
