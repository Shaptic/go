package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/stellar/go/exp/lighthorizon/index"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
)

type BatchConfig struct {
	historyarchive.Range
	TxMetaSourceUrl, TargetUrl string
}

func NewBatchConfigFromEnvironment() (*BatchConfig, error) {
	const (
		jobIndexEnv        = "AWS_BATCH_JOB_ARRAY_INDEX"
		firstCheckpointEnv = "FIRST_CHECKPOINT"
		batchSizeEnv       = "BATCH_SIZE"
		txmetaSourceUrlEnv = "TXMETA_SOURCE"
	)

	jobIndex, err := strconv.ParseUint(os.Getenv(jobIndexEnv), 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+jobIndexEnv)
	}

	firstCheckpoint, err := strconv.ParseUint(os.Getenv(firstCheckpointEnv), 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+firstCheckpointEnv)
	}

	batchSize, err := strconv.ParseUint(os.Getenv(batchSizeEnv), 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid parameter "+batchSizeEnv)
	}

	sourceUrl := os.Getenv(txmetaSourceUrlEnv)
	if sourceUrl == "" {
		return nil, errors.New("required parameter " + txmetaSourceUrlEnv)
	}

	startCheckpoint := uint32(firstCheckpoint + batchSize*jobIndex)
	endCheckpoint := startCheckpoint + uint32(batchSize) - 1
	return &BatchConfig{
		Range:           historyarchive.Range{Low: startCheckpoint, High: endCheckpoint},
		TargetUrl:       fmt.Sprintf("s3://some-url/job_%d?region=%s", jobIndex, "us-east-1"),
		TxMetaSourceUrl: sourceUrl,
	}, nil
}

func main() {
	log.SetLevel(log.InfoLevel)

	batch, err := NewBatchConfigFromEnvironment()
	if err != nil {
		panic(err)
	}

	log.Infof("Uploading ledger range [%d, %d] to %s",
		batch.Range.Low, batch.Range.High, batch.TargetUrl)

	if err := index.BuildIndices(
		context.Background(),
		batch.TxMetaSourceUrl,
		batch.TargetUrl,
		network.TestNetworkPassphrase,
		batch.Low, batch.High,
		[]string{"transactions", "accounts_unbacked"},
		1,
	); err != nil {
		panic(err)
	}
}
