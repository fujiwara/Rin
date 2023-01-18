package rin

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

type SQSBatchResponse struct {
	BatchItemFailures []BatchItemFailureItem `json:"batchItemFailures,omitempty"`
}

type BatchItemFailureItem struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

func runLambdaHandler(opt *Option) error {
	if opt.BatchMode {
		log.Printf("[info] starting lambda handler SQS batch mode")
		lambda.Start(newLambdaSQSBatchHandler(opt))
	} else {
		log.Printf("[info] starting lambda handler SQS event mode")
		lambda.Start(lambdaSQSEventHandler)
	}
	return nil
}

func lambdaSQSEventHandler(ctx context.Context, event *events.SQSEvent) (*SQSBatchResponse, error) {
	resp := &SQSBatchResponse{
		BatchItemFailures: nil,
	}
	for _, record := range event.Records {
		if record.MessageId == "" {
			return nil, errors.New("sqs message id is empty")
		}
		if err := processEvent(ctx, record.MessageId, record.Body); err != nil {
			resp.BatchItemFailures = append(resp.BatchItemFailures, BatchItemFailureItem{
				ItemIdentifier: record.MessageId,
			})
		}
	}
	return resp, nil
}

func newLambdaSQSBatchHandler(opt *Option) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		var wg sync.WaitGroup
		wg.Add(1)
		err := sqsWorker(ctx, &wg, opt)
		wg.Done()
		return err
	}
}
