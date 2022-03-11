package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var config *Config
var MaxDeleteRetry = 8
var Sessions = &SessionStore{}

type SessionStore struct {
	SQS      *session.Session
	Redshift *session.Session
	S3       *session.Session
}

var TrapSignals = []os.Signal{
	syscall.SIGHUP,
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGQUIT,
}

type NoMessageError struct {
	s string
}

func (e NoMessageError) Error() string {
	return e.s
}

func DryRun(configFile string, batchMode bool) error {
	var err error
	log.Println("[info] Loading config:", configFile)
	config, err = LoadConfig(configFile)
	if err != nil {
		return err
	}
	for _, target := range config.Targets {
		log.Println("[info] Define target", target.String())
	}
	return nil
}

func Run(configFile string, batchMode bool) error {
	return RunWithContext(context.Background(), configFile, batchMode)
}

func RunWithContext(ctx context.Context, configFile string, batchMode bool) error {
	var err error
	log.Println("[info] Loading config:", configFile)
	config, err = LoadConfig(configFile)
	if err != nil {
		return err
	}
	for _, target := range config.Targets {
		log.Println("[info] Define target", target.String())
	}

	if Sessions.SQS == nil {
		c := &aws.Config{
			Region: aws.String(config.Credentials.AWS_REGION),
		}
		if config.Credentials.AWS_ACCESS_KEY_ID != "" {
			c.Credentials = credentials.NewStaticCredentials(
				config.Credentials.AWS_ACCESS_KEY_ID,
				config.Credentials.AWS_SECRET_ACCESS_KEY,
				"",
			)
		}
		sess := session.Must(session.NewSession(c))
		Sessions.SQS = sess
		Sessions.Redshift = sess
		Sessions.S3 = sess
	}
	sqsSvc := sqs.New(Sessions.SQS)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, TrapSignals...)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2) // signal handler + sqsWorker

	// wait for signal
	go func() {
		defer wg.Done()
		sig := <-signalCh
		log.Printf("[info] Got signal: %s(%d)", sig, sig)
		log.Println("[info] Shutting down worker...")
		cancel()
	}()

	// run worker
	err = sqsWorker(ctx, &wg, sqsSvc, batchMode)

	wg.Wait()
	log.Println("[info] Shutdown.")
	if ctx.Err() == context.Canceled {
		// normally exit
		return nil
	}
	return err
}

func waitForRetry() {
	log.Println("[warn] Retry after 10 sec.")
	time.Sleep(10 * time.Second)
}

func sqsWorker(ctx context.Context, wg *sync.WaitGroup, svc *sqs.SQS, batchMode bool) error {
	var mode string
	if batchMode {
		mode = "Batch"
	} else {
		mode = "Worker"
	}
	log.Printf("[info] Starting up SQS %s", mode)
	defer log.Printf("[info] Shutdown SQS %s", mode)
	defer wg.Done()

	log.Println("[info] Connect to SQS:", config.QueueName)
	res, err := svc.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(config.QueueName),
	})
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := handleMessage(ctx, svc, res.QueueUrl); err != nil {
			if _, ok := err.(NoMessageError); ok {
				if batchMode {
					break
				} else {
					continue
				}
				if ctx.Err() == context.Canceled {
					return nil
				}
				if !batchMode {
					waitForRetry()
				}
			}
		}
	}
	return nil
}

func handleMessage(ctx context.Context, svc *sqs.SQS, queueUrl *string) error {
	var completed = false
	res, err := svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            queueUrl,
	})
	if err != nil {
		return err
	}
	if len(res.Messages) == 0 {
		return NoMessageError{"No messages"}
	}
	msg := res.Messages[0]
	msgId := *msg.MessageId
	log.Printf("[info] [%s] Starting process message.", msgId)
	log.Printf("[debug] [%s] handle: %s", msgId, *msg.ReceiptHandle)
	log.Printf("[debug] [%s] body: %s", msgId, *msg.Body)

	defer func() {
		if !completed {
			log.Printf("[info] [%s] Aborted message. ReceiptHandle: %s", msgId, *msg.ReceiptHandle)
		}
	}()

	event, err := ParseEvent([]byte(*msg.Body))
	if err != nil {
		log.Printf("[error] [%s] Can't parse event from Body. %s", msgId, err)
		return err
	}
	if event.IsTestEvent() {
		log.Printf("[info] [%s] Skipping %s", msgId, event.String())
	} else {
		log.Printf("[info] [%s] Importing event: %s", msgId, event)
		n, err := Import(event)
		if err != nil {
			log.Printf("[error] [%s] Import failed. %s", msgId, err)
			return err
		}
		if n == 0 {
			log.Printf("[warn] [%s] All events were not matched for any targets. Ignored.", msgId)
		} else {
			log.Printf("[info] [%s] %d actions completed.", msgId, n)
		}
	}
	_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		log.Printf("[warn] [%s] Can't delete message. %s", msgId, err)
		// retry
		for i := 1; i <= MaxDeleteRetry; i++ {
			log.Printf("[info] [%s] Retry to delete after %d sec.", msgId, i*i)
			time.Sleep(time.Duration(i*i) * time.Second)
			_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      queueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err == nil {
				log.Printf("[info] [%s] Message was deleted successfuly.", msgId)
				break
			}
			log.Printf("[warn] [%s] Can't delete message. %s", msgId, err)
			if i == MaxDeleteRetry {
				log.Printf("[error] [%s] Max retry count reached. Giving up.", msgId)
			}
		}
	}

	completed = true
	log.Printf("[info] [%s] Completed message.", msgId)
	return nil
}
