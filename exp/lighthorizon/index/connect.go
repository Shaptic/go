package index

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
)

func Connect(backendUrl string) (Store, error) {
	parsed, err := url.Parse(backendUrl)
	if err != nil {
		return nil, err
	}
	switch parsed.Scheme {
	case "s3":
		var parallel uint32 = 20
		config := &aws.Config{}
		query := parsed.Query()
		if region := query.Get("region"); region != "" {
			config.Region = aws.String(region)
		}

		// Somewhat of a hack: allow control of parallelization via a special
		// query parameter in the s3:// path.
		if workers := query.Get("workers"); workers != "" {
			workerCount, err := strconv.ParseUint(workers, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse worker count (%s): %v", workers, err)
			}
			if workerCount > 0 {
				parallel = uint32(workerCount)
			}
			query.Del("workers")
		}

		return NewS3Store(config, parsed.Path, parallel)

	case "file":
		return NewFileStore(filepath.Join(parsed.Host, parsed.Path), 20)

	default:
		return nil, errors.New("unknown URL scheme: '" + parsed.Scheme + "'")
	}
}
