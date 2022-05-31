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
	parallel = uint32(16)
)

type SafeStringSet struct {
	lock sync.Mutex
	set  map[string]struct{}
}

type ReduceConfig struct {
	JobIndex       uint32
	MapJobCount    uint32
	ReduceJobCount uint32
}

func ReduceConfigFromEnvironment() (*ReduceConfig, error) {
	const (
		jobIndexEnv   = "AWS_BATCH_JOB_ARRAY_INDEX"
		mapJobsEnv    = "MAP_JOBS"
		reduceJobsEnv = "REDUCE_JOBS"
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

func main() {
	log.SetLevel(log.InfoLevel)

	const (
		s3Region = "us-east-1"
		s3Path   = "some-path"
	)

	doneAccounts := NewSafeStringSet()

	config, err := ReduceConfigFromEnvironment()
	if err != nil {
		panic(err)
	}

	url := fmt.Sprintf("s3://%s/?region=%s", s3Path, s3Region)
	indexStore, err := index.Connect(url)
	if err != nil {
		panic(err)
	}

	for i := uint32(0); i < config.MapJobCount; i++ {
		url = fmt.Sprintf("s3://%s/job_%d?region=%s", s3Path, i, s3Region)
		outerJobStore, err := index.Connect(url)
		if err != nil {
			panic(err)
		}

		accounts, err := outerJobStore.ReadAccounts()
		if err != nil {
			// TODO: in final version this should be critical error, now just skip it
			if err == os.ErrNotExist {
				log.Errorf("Job %d is unavailable - TODO fix", i)
				continue
			}
			panic(err)
		}

		log.Info("Outer job ", i, " accounts ", len(accounts))

		ch := make(chan string, parallel)
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

		wg.Add(int(parallel))
		for j := uint32(0); j < parallel; j++ {
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
						routineIndex, parallel,
					) {
						skipped++
						continue
					}

					outerAccountIndexes, err := outerJobStore.Read(account)
					if err != nil {
						// TODO: in final version this should be critical error, now just skip it
						if err == os.ErrNotExist {
							log.Errorf("Account %s is unavailable - TODO fix", account)
							continue
						}
						panic(err)
					}

					for k := uint32(i + 1); k < config.MapJobCount; k++ {
						innerJobStore, err := index.NewS3Store(
							&aws.Config{Region: aws.String(s3Region)},
							fmt.Sprintf("job_%d", k),
							parallel,
						)
						if err != nil {
							panic(err)
						}

						innerAccountIndexes, err := innerJobStore.Read(account)
						if err != nil {
							if err == os.ErrNotExist {
								continue
							}
							panic(err)
						}

						for name, index := range outerAccountIndexes {
							if innerAccountIndexes[name] == nil {
								continue
							}
							err := index.Merge(innerAccountIndexes[name])
							if err != nil {
								panic(err)
							}
						}
					}

					// Save merged index
					indexStore.AddParticipantToIndexesNoBackend(account, outerAccountIndexes)

					// Mark account as done
					doneAccounts.Add(account)
					processed++

					if processed%200 == 0 {
						log.Infof("Flushing %d, processed %d", routineIndex, processed)
						err = indexStore.Flush()
						if err != nil {
							panic(err)
						}
					}
				}

				log.Infof("Flushing Accounts %d, processed %d", routineIndex, processed)
				err = indexStore.Flush()
				if err != nil {
					panic(err)
				}

				// Merge the transaction indexes
				// There's 256 files, (one for each first byte of the txn hash)
				processed = 0
				for i := byte(0x00); i < 0xff; i++ {
					if !shouldTransactionBeProcessed(i,
						config.JobIndex, config.ReduceJobCount,
						routineIndex, parallel,
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
							parallel,
						)
						if err != nil {
							panic(err)
						}

						innerTxnIndexes, err := innerJobStore.ReadTransactions(prefix)
						if err != nil {
							if err == os.ErrNotExist {
								continue
							}
							panic(err)
						}

						if err := indexStore.MergeTransactions(prefix, innerTxnIndexes); err != nil {
							panic(err)
						}
					}
				}

				log.Infof("Flushing Transactions %d, processed %d", routineIndex, processed)
				err = indexStore.Flush()
				if err != nil {
					panic(err)
				}
			}(j)
		}

		wg.Wait()
	}
}

func NewSafeStringSet() *SafeStringSet {
	return &SafeStringSet{
		lock: sync.Mutex{},
		set:  map[string]struct{}{},
	}
}

func (set *SafeStringSet) Contains(key string) bool {
	defer set.lock.Unlock()
	set.lock.Lock()
	_, ok := set.set[key]
	return ok
}

func (set *SafeStringSet) Add(key string) {
	defer set.lock.Unlock()
	set.lock.Lock()
	set.set[key] = struct{}{}
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
	hashLeft := uint32(transactionPrefix >> 4)
	hashRight := uint32(0x0F & transactionPrefix)

	// Because the transaction hash (and thus the first byte or "prefix") is a
	// random value, its remainders w.r.t. the indices will distribute the work
	// fairly (and deterministically).
	return hashRight%jobCount == jobIndex &&
		hashLeft%routineCount == routineIndex
}
