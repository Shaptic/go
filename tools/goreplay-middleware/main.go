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

	"github.com/buger/goreplay/proto"
	"github.com/stellar/go/support/log"
)

const (
	requestType          byte = '1'
	originalResponseType byte = '2'
	replayedResponseType byte = '3'
)

var pendingRequestsAdded int64
var pendingRequests map[string]*Request

func main() {
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		for range ticker.C {
			os.Stderr.WriteString(fmt.Sprintf("Middleware stats: pendingRequests=%d pendingRequestsAdded=%d\n", len(pendingRequests), pendingRequestsAdded))
		}
	}()

	processAll(os.Stdin, os.Stderr, os.Stdout)
}

func init() {
	pendingRequests = make(map[string]*Request)
}

func processAll(stdin io.Reader, stderr, stdout io.Writer) {
	log.SetOut(stderr)
	log.SetLevel(log.InfoLevel)

	scanner := bufio.NewScanner(stdin)
	buf := make([]byte, 20*1024*1024) // 20MB
	scanner.Buffer(buf, 20*1024*1024)

	for scanner.Scan() {
		encoded := scanner.Bytes()
		buf := make([]byte, len(encoded)/2)
		_, err := hex.Decode(buf, encoded)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("hex.Decode error: %v", err))
			continue
		}

		if err := scanner.Err(); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("scanner.Err(): %v\n", err))
		}

		process(stderr, stdout, buf)

		if len(pendingRequests) > 2000 {
			// Around 3-4% of responses is lost (not sure why) so pendingRequests can grow
			// indefinietly. Let's just truncate it when it becomes too big.
			// There is one gotcha here. Goreplay will still send requests
			// (`1` type payloads) even if traffic is rate limited. So if rate
			// limit is applied even more requests can be lost. So TODO: we should
			// implement rate limiting here when using middleware rather than
			// using Goreplay rate limit.
			pendingRequests = make(map[string]*Request)
		}
	}
}

func process(stderr, stdout io.Writer, buf []byte) {
	// First byte indicate payload type:
	payloadType := buf[0]
	headerSize := bytes.IndexByte(buf, '\n') + 1
	header := buf[:headerSize-1]

	// Header contains space separated values of: request type, request id, and request start time (or round-trip time for responses)
	meta := bytes.Split(header, []byte(" "))
	// For each request you should receive 3 payloads (request, response, replayed response) with same request id
	reqID := string(meta[1])
	payload := buf[headerSize:]

	switch payloadType {
	case requestType:
		pendingRequests[reqID] = &Request{
			Headers: payload,
		}
		pendingRequestsAdded++

		// Emitting data back, without modification
		_, err := io.WriteString(stdout, hex.EncodeToString(buf)+"\n")
		if err != nil {
			io.WriteString(stderr, fmt.Sprintf("stdout.WriteString error: %v", err))
		}
	case originalResponseType:
		if req, ok := pendingRequests[reqID]; ok {
			// Token is inside response body
			req.OriginalResponse = payload
		}
	case replayedResponseType:
		if req, ok := pendingRequests[reqID]; ok {
			req.MirroredResponse = payload

			if !req.ResponseEquals() {
				// TODO in the future publish the results to S3 for easier processing
				// stderr.WriteString("MISMATCH " + req.SerializeBase64() + "\n")
				log.WithFields(log.F{
					"expected": req.OriginalBody(),
					"actual":   req.MirroredBody(),
					"headers":  string(req.Headers),
					"path":     string(proto.Path(req.Headers)),
				}).Info("Mismatch found")
			}

			delete(pendingRequests, reqID)
		}
	default:
		io.WriteString(stderr, "Unknown message type\n")
	}
}
