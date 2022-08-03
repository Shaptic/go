package main

import (
	"fmt"
	"regexp"

	"github.com/buger/goreplay/proto"
)

var horizonURLs = regexp.MustCompile(`"result_meta_xdr":[ ]?"([^"]*)",`)
var findResultMetaXDR = regexp.MustCompile(`https:\/\/.*?stellar\.org`)

// removeRegexps contains a list of regular expressions that, when matched,
// will be changed to an empty string. This is done to exclude known
// differences in responses between two Horizon version.
//
// Let's say that next Horizon version adds a new bool field:
// `is_authorized` on account balances list. You want to remove this
// field so it's not reported for each `/accounts/{id}` response.
var removeRegexps = []*regexp.Regexp{}

type replace struct {
	regexp *regexp.Regexp
	repl   string
}

// replaceRegexps works like removeRegexps but replaces data
var replaceRegexps = []replace{}

type Request struct {
	Headers          string
	OriginalResponse []byte
	MirroredResponse []byte
}

func (r *Request) ResponseEquals() bool {
	// TODO fast fail on `Latest-Ledger` mismatch

	originalBody := proto.Body(r.OriginalResponse)
	mirroredBody := proto.Body(r.MirroredResponse)

	return normalizeResponseBody(originalBody) == normalizeResponseBody(mirroredBody)
}

// normalizeResponseBody normalizes body to allow byte-byte comparison like removing
// URLs from _links or tx meta. May require updating on new releases.
func normalizeResponseBody(body []byte) string {
	normalizedBody := string(body)
	// `result_meta_xdr` can differ between core instances (confirmed this with core team)
	normalizedBody = findResultMetaXDR.ReplaceAllString(normalizedBody, "")
	// Remove Horizon URL from the _links
	normalizedBody = horizonURLs.ReplaceAllString(normalizedBody, "")

	for _, reg := range removeRegexps {
		normalizedBody = reg.ReplaceAllString(normalizedBody, "")
	}

	for _, reg := range replaceRegexps {
		normalizedBody = reg.regexp.ReplaceAllString(normalizedBody, reg.repl)
	}

	return normalizedBody
}

func (r *Request) SerializeBase64() string {
	return fmt.Sprintf(
		"headers: %s original: %s mirrored: %s",
		r.Headers,
		string(r.OriginalResponse),
		string(r.MirroredResponse),
	)
}
