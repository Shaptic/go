package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/stellar/go/exp/lighthorizon/index"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
)

const (
	ACCOUNT_FLUSH_FREQUENCY = 200
	// arbitrary default, should we use runtime.NumCPU()?
	DEFAULT_WORKER_COUNT = 2
)

type ReduceConfig struct {
	JobIndex        uint32
	MapJobCount     uint32
	ReduceJobCount  uint32
	IndexTarget     string
	IndexRootSource string

	Workers uint32
}

func ReduceConfigFromEnvironment() (*ReduceConfig, error) {
	const (
		mapJobsEnv          = "MAP_JOB_COUNT"
		reduceJobsEnv       = "REDUCE_JOB_COUNT"
		workerCountEnv      = "WORKER_COUNT"
		jobIndexEnv         = "AWS_BATCH_JOB_ARRAY_INDEX"
		mappedIndicesUrlEnv = "INDEX_SOURCE_ROOT"
		finalIndexUrlEnv    = "INDEX_TARGET"
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

	workers := os.Getenv(workerCountEnv)
	if workers == "" {
		workers = strconv.FormatUint(DEFAULT_WORKER_COUNT, 10)
	}
	workerCount, err := strconv.ParseUint(workers, 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+workerCountEnv)
	}

	finalIndexUrl := os.Getenv(finalIndexUrlEnv)
	if finalIndexUrl == "" {
		return nil, errors.New("required parameter missing " + finalIndexUrlEnv)
	}

	mappedIndicesUrl := os.Getenv(mappedIndicesUrlEnv)
	if mappedIndicesUrl == "" {
		return nil, errors.New("required parameter missing " + mappedIndicesUrlEnv)
	}

	return &ReduceConfig{
		JobIndex:        uint32(jobIndex),
		MapJobCount:     uint32(mapJobs),
		ReduceJobCount:  uint32(reduceJobs),
		Workers:         uint32(workerCount),
		IndexTarget:     finalIndexUrl,
		IndexRootSource: mappedIndicesUrl,
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

	finalIndexStore, err := index.Connect(config.IndexTarget)
	if err != nil {
		panic(errors.Wrapf(err, "failed to connect to indices at %s",
			config.IndexTarget))
	}

	if err := mergeAllIndices(finalIndexStore, config); err != nil {
		panic(errors.Wrap(err, "failed to merge indices"))
	}
}

func mergeAllIndices(finalIndexStore index.Store, config *ReduceConfig) error {
	doneAccounts := NewSafeStringSet()
	for i := uint32(0); i < config.MapJobCount; i++ {
		logger := log.WithField("worker", i)

		url := filepath.Join(config.IndexRootSource, "job_"+strconv.FormatUint(uint64(i), 10))
		logger.Infof("Connecting to %s", url)

		outerJobStore, err := index.Connect(url)
		if err != nil {
			return errors.Wrapf(err, "failed to connect to indices at %s", url)
		}

		accounts, err := outerJobStore.ReadAccounts()
		// TODO: in final version this should be critical error, now just skip it
		if os.IsNotExist(err) {
			logger.Errorf("accounts file not found (TODO!)")
			continue
		} else if err != nil {
			return errors.Wrapf(err, "failed to read accounts for worker %d", i)
		}

		logger.Info("Accounts to process: ", len(accounts))

		ch := make(chan string, config.Workers)
		go func() {
			for _, account := range accounts {
				if doneAccounts.Contains(account) || account == "" {
					// Account index already merged in the previous outer job
					continue
				}
				ch <- account
			}
			close(ch)
		}()

		// TODO: errgroup.WithContext(ctx)
		var wg sync.WaitGroup
		wg.Add(int(config.Workers))
		for j := uint32(0); j < config.Workers; j++ {
			go func(routineIndex uint32) {
				defer wg.Done()
				logger := log.WithField("worker", i).WithField("routine", routineIndex)
				logger.Info("Processing accounts...")

				var accountsProcessed, accountsSkipped uint64
				for account := range ch {
					if (accountsProcessed+accountsSkipped)%97 == 0 {
						logger.
							WithField("indexed", accountsProcessed).
							WithField("skipped", accountsSkipped).
							WithField("total", len(accounts)).
							Infof("Processed %d/%d accounts",
								accountsProcessed+accountsSkipped, len(accounts))
					}

					if !shouldAccountBeProcessed(account,
						config.JobIndex, config.ReduceJobCount,
						routineIndex, config.Workers,
					) {
						// logger.Debugf("Skipping '%s'", account)
						accountsSkipped++
						continue
					}

					logger.Debugf("Reading indices for account '%s'", account)
					outerAccountIndices, err := outerJobStore.Read(account)
					// TODO: in final version this should be critical error, now just skip it
					if os.IsNotExist(err) {
						logger.Errorf("Account %s is unavailable - TODO fix", account)
						continue
					} else if err != nil {
						panic(err)
					}

					for k := uint32(i); k <= config.MapJobCount; k++ {
						url := filepath.Join(config.IndexRootSource, fmt.Sprintf("job_%d", k))
						innerJobStore, err := index.Connect(url)
						if err != nil {
							logger.Errorf("Failed to connect to indices at %s: %v", url, err)
							panic(err)
						}

						innerAccountIndices, err := innerJobStore.Read(account)
						if os.IsNotExist(err) {
							continue
						} else if err != nil {
							logger.Errorf("Failed to read indices: %v", err)
							panic(err)
						}

						for name, index := range outerAccountIndices {
							innerIndices, ok := innerAccountIndices[name]
							if !ok || innerIndices == nil {
								continue
							}

							if err := index.Merge(innerIndices); err != nil {
								logger.Errorf("Failed to merge indices for %s: %v", name, err)
								panic(err)
							}
						}
					}

					// Save merged index
					finalIndexStore.AddParticipantToIndexesNoBackend(account, outerAccountIndices)

					// Mark account as done
					doneAccounts.Add(account)
					accountsProcessed++

					if accountsProcessed%ACCOUNT_FLUSH_FREQUENCY == 0 {
						logger.Infof("Flushing %d indexed accounts.", accountsProcessed)

						if err = finalIndexStore.Flush(); err != nil {
							logger.WithField("error", err).Errorf("Flush error.")
							panic(err)
						}
					}
				}

				logger.Infof("Final account flush (%d processed)...", accountsProcessed)
				if err = finalIndexStore.Flush(); err != nil {
					logger.WithField("error", err).Errorf("Flush error.")
					panic(err)
				}

				// Merge the transaction indexes
				// There's 256 files, (one for each first byte of the txn hash)
				var transactionsProcessed, transactionsSkipped uint64
				for i := byte(0x00); i < 0xff; i++ {
					logger.Infof("%d transactions processed (%d skipped)",
						transactionsProcessed, transactionsSkipped)

					if !shouldTransactionBeProcessed(i,
						config.JobIndex, config.ReduceJobCount,
						routineIndex, workerCount,
					) {
						transactionsSkipped++
						continue
					}
					transactionsProcessed++

					prefix := hex.EncodeToString([]byte{i})

					for k := uint32(0); k < config.MapJobCount; k++ {
						url := path.Join(config.IndexRootSource, fmt.Sprintf("job_%d", k))
						innerJobStore, err := index.Connect(url)
						if err != nil {
							logger.Errorf("Error connecting to indices at %s", url)
							panic(err)
						}

						innerTxnIndexes, err := innerJobStore.ReadTransactions(prefix)
						if err == os.ErrNotExist {
							continue
						} else if err != nil {
							logger.Errorf("Error reading transactions: %v", err)
							panic(err)
						}

						if err := finalIndexStore.MergeTransactions(prefix, innerTxnIndexes); err != nil {
							logger.Errorf("Error merging transactions: %v", err)
							panic(err)
						}
					}
				}

				logger.Infof("Final transaction flush (%d processed)", transactionsProcessed)
				if err = finalIndexStore.Flush(); err != nil {
					logger.Errorf("Error flushing transactions: %v", err)
					panic(err)
				}
			}(j)
		}

		wg.Wait()
	}

	return nil
}

func (cfg *ReduceConfig) shouldProcessAccount(account string, routineIndex uint32) bool {
	hash := fnv.New64a()

	// Docs state (https://pkg.go.dev/hash#Hash) that Write will never error.
	hash.Write([]byte(account))
	digest := uint32(hash.Sum64()) // discard top 32 bits

	leftHalf := digest >> 16
	rightHalf := digest & 0x0000FFFF

	log.WithField("worker", routineIndex).
		WithField("account", account).
		Debugf("Hash: %d (left=%d, right=%d)", digest, leftHalf, rightHalf)

	// Because the digest is basically a random number (given a good hash
	// function), its remainders w.r.t. the indices will distribute the work
	// fairly (and deterministically).
	return leftHalf%cfg.ReduceJobCount == cfg.JobIndex &&
		rightHalf%cfg.Workers == routineIndex
}

func (cfg *ReduceConfig) shouldProcessTx(txPrefix byte, routineIndex uint32) bool {
	hashLeft := uint32(transactionPrefix >> 4)
	hashRight := uint32(transactionPrefix & 0x0F)

	// Because the transaction hash (and thus the first byte or "prefix") is a
	// random value, its remainders w.r.t. the indices will distribute the work
	// fairly (and deterministically).
	return hashRight%cfg.ReduceJobCount == cfg.JobIndex &&
		hashLeft%cfg.Workers == routineIndex
}

// For every index that exists in `dest`, finds the corresponding index in
// `source` and merges it into `dest`'s version.
func mergeIndices(dest, source map[string]*index.CheckpointIndex) error {
	for name, index := range dest {
		// The source doesn't contain this particular index.
		//
		// This probably shouldn't happen, since during the Map step, there's no
		// way to choose which indices you want, but, strictly-speaking, it's
		// not an error, so we can just move on.
		innerIndices, ok := source[name]
		if !ok || innerIndices == nil {
			continue
		}

		if err := index.Merge(innerIndices); err != nil {
			return errors.Wrapf(err, "failed to merge index for %s", name)
		}
	}

	return nil
}
