package services

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/stellar/go/exp/lighthorizon/archive"
	"github.com/stellar/go/exp/lighthorizon/common"
	"github.com/stellar/go/exp/lighthorizon/index"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/go/support/log"
)

const (
	allTransactionsIndex = "all/all"
	allPaymentsIndex     = "all/payments"
)

var (
	checkpointManager = historyarchive.NewCheckpointManager(0)
)

type LightHorizon struct {
	Operations   OperationsService
	Transactions TransactionsService
}

type Config struct {
	Archive    archive.Archive
	IndexStore index.Store
	Passphrase string
}

type TransactionsService struct {
	TransactionRepository
	Config Config
}

type OperationsService struct {
	OperationsRepository,
	Config Config
}

type OperationsRepository interface {
	GetOperationsByAccount(ctx context.Context, cursor int64, limit uint64, accountId string) ([]common.Operation, error)
}

type TransactionRepository interface {
	GetTransactionsByAccount(ctx context.Context, cursor int64, limit uint64, accountId string) ([]common.Transaction, error)
}

// searchCallback is a generic way for any endpoint to process a transaction and
// its corresponding ledger. It should return whether or not we should stop
// processing (e.g. when a limit is reached) and any error that occurred.
type searchCallback func(archive.LedgerTransaction, *xdr.LedgerHeader) (finished bool, err error)

func (os *OperationsService) GetOperationsByAccount(ctx context.Context,
	cursor int64, limit uint64,
	accountId string,
) ([]common.Operation, error) {
	ops := []common.Operation{}

	opsCallback := func(tx archive.LedgerTransaction, ledgerHeader *xdr.LedgerHeader) (bool, error) {
		for operationOrder, op := range tx.Envelope.Operations() {
			opParticipants, err := os.Config.Archive.GetOperationParticipants(tx, op, operationOrder)
			if err != nil {
				return false, err
			}

			if _, foundInOp := opParticipants[accountId]; foundInOp {
				ops = append(ops, common.Operation{
					TransactionEnvelope: &tx.Envelope,
					TransactionResult:   &tx.Result.Result,
					LedgerHeader:        ledgerHeader,
					TxIndex:             int32(tx.Index),
					OpIndex:             int32(operationOrder),
				})

				if uint64(len(ops)) >= limit {
					return true, nil
				}
			}
		}

		return false, nil
	}

	if err := searchTxByAccount(ctx, cursor, accountId, os.Config, opsCallback); err != nil {
		return nil, err
	}

	return ops, nil
}

func (ts *TransactionsService) GetTransactionsByAccount(ctx context.Context,
	cursor int64, limit uint64,
	accountId string,
) ([]common.Transaction, error) {
	txs := []common.Transaction{}

	txsCallback := func(tx archive.LedgerTransaction, ledgerHeader *xdr.LedgerHeader) (bool, error) {
		txs = append(txs, common.Transaction{
			TransactionEnvelope: &tx.Envelope,
			TransactionResult:   &tx.Result.Result,
			LedgerHeader:        ledgerHeader,
			TxIndex:             int32(tx.Index),
			NetworkPassphrase:   ts.Config.Passphrase,
		})

		return (uint64(len(txs)) >= limit), nil
	}

	start := time.Now()
	defer func() {
		log.WithField("duration", time.Since(start)).
			WithField("limit", limit).
			WithField("cursor", cursor).
			Infof("Request fulfilled.")
	}()

	if err := searchTxByAccount(ctx, cursor, accountId, ts.Config, txsCallback); err != nil {
		return nil, err
	}
	return txs, nil
}

func searchTxByAccount(ctx context.Context,
	cursor int64, accountId string,
	config Config,
	callback searchCallback,
) error {
	cursorMgr := NewCursorManagerForAccountActivity(config.IndexStore, accountId)
	cursor, err := cursorMgr.Begin(cursor)
	if err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	nextLedger := getLedgerFromCursor(cursor)

	log.WithField("cursor", cursor).
		Infof("Searching index by account %s starting at ledger %d",
			accountId, nextLedger)

	ctx, cancel := context.WithCancel(ctx)
	start := time.Now()
	defer cancel()

	for {
		ledgerReader := make(chan xdr.LedgerCloseMeta, 1)
		LedgerFeeder(ctx, config.Archive,
			ledgerReader,
			historyarchive.Range{
				Low:  nextLedger,
				High: checkpointManager.GetCheckpoint(nextLedger),
			},
			4,
		)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		for ledger := range ledgerReader {
			reader, readerErr := config.Archive.NewLedgerTransactionReaderFromLedgerCloseMeta(config.Passphrase, ledger)
			if readerErr != nil {
				return readerErr
			}

			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				tx, readErr := reader.Read()
				if readErr == io.EOF {
					break
				} else if readErr != nil {
					return readErr
				}

				participants, participantErr := config.Archive.GetTransactionParticipants(tx)
				if participantErr != nil {
					return participantErr
				}

				if _, found := participants[accountId]; found {
					finished, callBackErr := callback(tx, &ledger.V0.LedgerHeader.Header)
					if finished || callBackErr != nil {
						return callBackErr
					}
				}
			}

			log.WithField("duration", time.Since(start)).
				Infof("Done reading ledger %d", ledger.LedgerSequence())
		}

		cursor, err = cursorMgr.Advance()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		nextLedger = getLedgerFromCursor(cursor)
	}
}

func getLedgerFromCursor(cursor int64) uint32 {
	return uint32(toid.Parse(cursor).LedgerSequence)
}

// LedgerFeeder allows parallel downloads of ledgers via an archive. You provide
// it with a ledger range and a channel to which to output ledgers, and it will
// feed ledgers to it in sequential order while downloading up to
// `downloadWorkerCount` of them in parallel.
func LedgerFeeder(
	ctx context.Context,
	ledgerArchive archive.Archive,
	outputChan chan<- xdr.LedgerCloseMeta,
	ledgerRange historyarchive.Range,
	downloadWorkerCount int,
) *errgroup.Group {
	count := (ledgerRange.High - ledgerRange.Low) + 1
	ledgerFeed := make([]*xdr.LedgerCloseMeta, count)
	workQueue := make(chan uint32, downloadWorkerCount)

	log.Infof("Preparing %d parallel downloads of range: [%d, %d] (%d ledgers)",
		downloadWorkerCount, ledgerRange.Low, ledgerRange.High, count)
	wg, ctx := errgroup.WithContext(ctx)

	// This work publisher adds ledger sequence numbers to the work queue.
	wg.Go(func() error {
		for seq := ledgerRange.Low; seq <= ledgerRange.High; seq++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			workQueue <- seq
		}

		close(workQueue)
		return nil
	})

	// This result publisher pushes txmetas to the output queue in order.
	lock := sync.Mutex{}
	cond := sync.NewCond(&lock)

	go func() {
		lastPublishedIdx := int64(-1)

		// Until the last ledger in the range has been published to the queue:
		//  - wait for the signal of a new ledger being available
		//  - ensure the ledger is sequential after the last one
		//  - increment and go again
		for lastPublishedIdx < int64(count)-1 {
			if ctx.Err() != nil {
				return
			}

			lock.Lock()
			before := lastPublishedIdx
			for ledgerFeed[before+1] == nil {
				cond.Wait()
			}

			outputChan <- *ledgerFeed[before+1]
			ledgerFeed[before+1] = nil // save memory
			lastPublishedIdx++
			lock.Unlock()
		}

		log.Debugf("Publisher done: %+v", wg.Wait())
		close(outputChan)
	}()

	// These are the workers that download & store ledgers in memory.
	for i := 0; i < downloadWorkerCount; i++ {
		wg.Go(func() error {
			for ledgerSeq := range workQueue {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				start := time.Now()
				txmeta, err := ledgerArchive.GetLedger(ctx, ledgerSeq)
				log.WithField("duration", time.Since(start)).
					Infof("Downloaded ledger %d", ledgerSeq)
				if err != nil {
					return err
				}

				ledgerFeed[ledgerSeq-ledgerRange.Low] = &txmeta
				cond.Signal()
			}

			return nil
		})
	}

	return wg
}
