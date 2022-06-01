package main

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stellar/go/exp/lighthorizon/index"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
)

var (
	// Should we use runtime.NumCPU() for a reasonable default?
	workerCount = uint32(16)
)

type ReduceConfig struct {
	JobIndex       uint32
	MapJobCount    uint32
	ReduceJobCount uint32
}

func ReduceConfigFromEnvironment() (*ReduceConfig, error) {
	const (
		mapJobsEnv    = "MAP_JOB_COUNT"
		reduceJobsEnv = "REDUCE_JOB_COUNT"
		jobIndexEnv   = "AWS_BATCH_JOB_ARRAY_INDEX"
	)

	jobIndex, err := strconv.ParseUint(os.Getenv(jobIndexEnv), 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+jobIndexEnv)
	}
	mapJobs, err := strconv.ParseUint(os.Getenv(mapJobsEnv), 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+mapJobsEnv)
	}
	reduceJobs, err := strconv.ParseUint(os.Getenv(reduceJobsEnv), 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+reduceJobsEnv)
	}

	return &ReduceConfig{
		JobIndex:       uint32(jobIndex),
		MapJobCount:    uint32(mapJobs),
		ReduceJobCount: uint32(reduceJobs),
	}, nil
}

const (
	s3Region = "us-east-1"
)

func main() {
	log.SetLevel(log.InfoLevel)

	config, err := ReduceConfigFromEnvironment()
	if err != nil {
		panic(err)
	}

	url := fmt.Sprintf("s3:///?region=%s", s3Region)
	finalIndexStore, err := index.Connect(url)
	if err != nil {
		panic(err)
	}

	if err := mergeAllIndices(finalIndexStore, config); err != nil {
		panic(err)
	}
}

func mergeAllIndices(finalIndexStore index.Store, config *ReduceConfig) error {
	doneAccounts := NewSafeStringSet()
	for i := uint32(0); i < config.MapJobCount; i++ {
		url := fmt.Sprintf("s3://job_%d/?region=%s", i, s3Region)
		outerJobStore, err := index.Connect(url)
		if err != nil {
			return errors.Wrapf(err, "failed to connect to indices at %s", url)
		}

		accounts, err := outerJobStore.ReadAccounts()
		if err != nil {
			// TODO: in final version this should be critical error, now just skip it
			if err == os.ErrNotExist {
				log.Errorf("Job %d is unavailable - TODO fix", i)
				continue
			}
			return errors.Wrapf(err, "failed to read accounts for job %d", i)
		}

		log.Info("Outer job ", i, " accounts ", len(accounts))

		ch := make(chan string, workerCount)
		go func() {
			for _, account := range accounts {
				if doneAccounts.Contains(account) {
					// Account index already merged in the previous outer job
					continue
				}
				ch <- account
			}
			close(ch)
		}()

		var wg sync.WaitGroup
		// TODO
		// ctx := context.Background()
		// wg, ctx := errgroup.WithContext(ctx)

		wg.Add(int(workerCount))
		for j := uint32(0); j < workerCount; j++ {
			go func(routineIndex uint32) {
				defer wg.Done()
				var skipped, processed uint64
				for account := range ch {
					// if (processed+skipped)%1000 == 0 {
					log.Infof(
						"outer: %d, routine: %d, processed: %d, skipped: %d, all account in outer job: %d\n",
						i, routineIndex, processed, skipped, len(accounts),
					)
					// }

					if !shouldAccountBeProcessed(account,
						config.JobIndex, config.ReduceJobCount,
						routineIndex, workerCount,
					) {
						skipped++
						continue
					}

					outerAccountIndices, err := outerJobStore.Read(account)
					if err != nil {
						// TODO: in final version this should be critical error, now just skip it
						if err == os.ErrNotExist {
							log.Errorf("Account %s is unavailable - TODO fix", account)
							continue
						}
						panic(err)
					}

					for k := uint32(i + 1); k < config.MapJobCount; k++ {
						url := fmt.Sprintf("s3://job_%d?region=%s&workers=%d",
							k, s3Region, workerCount)
						innerJobStore, err := index.Connect(url)
						if err != nil {
							panic(err)
						}

						innerAccountIndices, err := innerJobStore.Read(account)
						if err == os.ErrNotExist {
							continue
						} else if err != nil {
							panic(err)
						}

						for name, index := range outerAccountIndices {
							if innerAccountIndices[name] == nil {
								continue
							}
							err := index.Merge(innerAccountIndices[name])
							if err != nil {
								panic(err)
							}
						}
					}

					// Save merged index
					finalIndexStore.AddParticipantToIndexesNoBackend(account, outerAccountIndices)

					// Mark account as done
					doneAccounts.Add(account)
					processed++

					if processed%200 == 0 {
						log.Infof("Flushing %d, processed %d", routineIndex, processed)
						if err = finalIndexStore.Flush(); err != nil {
							panic(err)
						}
					}
				}

				log.Infof("Flushing Accounts %d, processed %d", routineIndex, processed)
				if err = finalIndexStore.Flush(); err != nil {
					panic(err)
				}

				// Merge the transaction indexes
				// There's 256 files, (one for each first byte of the txn hash)
				processed = 0
				for i := byte(0x00); i < 0xff; i++ {
					if !shouldTransactionBeProcessed(i,
						config.JobIndex, config.ReduceJobCount,
						routineIndex, workerCount,
					) {
						skipped++
						continue
					}
					processed++

					prefix := hex.EncodeToString([]byte{i})

					for k := uint32(0); k < config.MapJobCount; k++ {
						innerJobStore, err := index.NewS3Store(
							&aws.Config{Region: aws.String("us-east-1")},
							fmt.Sprintf("job_%d", k),
							workerCount,
						)
						if err != nil {
							panic(err)
						}

						innerTxnIndexes, err := innerJobStore.ReadTransactions(prefix)
						if err == os.ErrNotExist {
							continue
						} else if err != nil {
							panic(err)
						}

						if err := finalIndexStore.MergeTransactions(prefix, innerTxnIndexes); err != nil {
							panic(err)
						}
					}
				}

				log.Infof("Flushing Transactions %d, processed %d", routineIndex, processed)
				if err = finalIndexStore.Flush(); err != nil {
					panic(err)
				}
			}(j)
		}

		wg.Wait()
	}

	return nil
}

func shouldAccountBeProcessed(account string,
	jobIndex, jobCount uint32,
	routineIndex, routineCount uint32,
) bool {
	hash := fnv.New64a()

	// Docs state (https://pkg.go.dev/hash#Hash) that Write will never error.
	hash.Write([]byte(account))
	digest := uint32(hash.Sum64()) // discard top 32 bits

	leftHalf := digest >> 4
	rightHalf := digest & 0x0000FFFF

	// Because the digest is basically a random number (given a good hash
	// function), its remainders w.r.t. the indices will distribute the work
	// fairly (and deterministically).
	return leftHalf%jobCount == jobIndex &&
		rightHalf%routineCount == routineIndex
}

func shouldTransactionBeProcessed(transactionPrefix byte,
	jobIndex, jobCount uint32,
	routineIndex, routineCount uint32,
) bool {
	hashLeft := uint32(transactionPrefix & 0xF0)
	hashRight := uint32(0x0F & transactionPrefix)

	// Because the transaction hash (and thus the first byte or "prefix") is a
	// random value, its remainders w.r.t. the indices will distribute the work
	// fairly (and deterministically).
	return hashRight%jobCount == jobIndex &&
		hashLeft%routineCount == routineIndex
}
