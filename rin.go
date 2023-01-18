package rin

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	redshiftdatasqldriver "github.com/mashiike/redshift-data-sql-driver"
)

var config *Config
var MaxDeleteRetry = 8
var Sessions *SessionStore

func init() {
	Sessions = &SessionStore{}
	redshiftdatasqldriver.RedshiftDataClientConstructor = func(ctx context.Context, cfg *redshiftdatasqldriver.RedshiftDataConfig) (redshiftdatasqldriver.RedshiftDataClient, error) {
		return redshiftdata.NewFromConfig(*Sessions.Redshift, cfg.RedshiftDataOptFns...), nil
	}
}

type SessionStore struct {
	SQS            *aws.Config
	SQSOptFns      []func(*sqs.Options)
	Redshift       *aws.Config
	RedshiftOptFns []func(*redshift.Options)
	S3             *aws.Config
	S3OptFns       []func(*s3.Options)
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
	ctx := context.Background()
	var err error
	log.Println("[info] Loading config:", configFile)
	config, err = LoadConfig(ctx, configFile)
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
	config, err = LoadConfig(ctx, configFile)
	if err != nil {
		return err
	}
	for _, target := range config.Targets {
		log.Println("[info] Define target", target.String())
	}

	if Sessions.SQS == nil {
		opts := []func(*awsConfig.LoadOptions) error{
			awsConfig.WithRegion(config.Credentials.AWS_REGION),
		}
		if config.Credentials.AWS_ACCESS_KEY_ID != "" {
			opts = append(opts, awsConfig.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     config.Credentials.AWS_ACCESS_KEY_ID,
					SecretAccessKey: config.Credentials.AWS_SECRET_ACCESS_KEY,
					Source:          "from Rin config",
				},
			}))
		}
		c, err := awsConfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return err
		}
		Sessions.SQS = &c
		Sessions.SQSOptFns = make([]func(*sqs.Options), 0)
		Sessions.Redshift = &c
		Sessions.RedshiftOptFns = make([]func(*redshift.Options), 0)
		Sessions.S3 = &c
		Sessions.S3OptFns = make([]func(*s3.Options), 0)
	}

	if isLambda() {
		return runLambdaHandler(batchMode)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, TrapSignals...)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2) // signal handler + sqsWorker

	// wait for signal
	go func() {
		defer wg.Done()
		select {
		case sig := <-signalCh:
			log.Printf("[info] Got signal: %s(%d)", sig, sig)
			log.Println("[info] Shutting down worker...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// run worker
	err = sqsWorker(ctx, &wg, batchMode)

	wg.Wait()
	log.Println("[info] Shutdown.")
	if ctx.Err() == context.Canceled {
		// normally exit
		return nil
	}
	return err
}

func isLambda() bool {
	return strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") || os.Getenv("AWS_LAMBDA_RUNTIME_API") != ""
}

func sqsWorker(ctx context.Context, wg *sync.WaitGroup, batchMode bool) error {
	svc := sqs.NewFromConfig(*Sessions.SQS, Sessions.SQSOptFns...)
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
	res, err := svc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
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
			if e, ok := err.(NoMessageError); ok {
				if batchMode {
					log.Printf("[info] %s. Exit.", e.Error())
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	return nil
}

func handleMessage(ctx context.Context, svc *sqs.Client, queueUrl *string) error {
	var completed = false
	res, err := svc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 1,
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

	if err := processEvent(ctx, msgId, *msg.Body); err != nil {
		return err
	}

	ctxDelete, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err = svc.DeleteMessage(ctxDelete, &sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		log.Printf("[warn] [%s] Can't delete message. %s", msgId, err)
		// retry
		for i := 1; i <= MaxDeleteRetry; i++ {
			log.Printf("[info] [%s] Retry to delete after %d sec.", msgId, i*i)
			time.Sleep(time.Duration(i*i) * time.Second)
			_, err = svc.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
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

func processEvent(ctx context.Context, msgId string, body string) error {
	event, err := ParseEvent([]byte(body))
	if err != nil {
		log.Printf("[error] [%s] Can't parse event from Body. %s", msgId, err)
		return err
	}
	if event.IsTestEvent() {
		log.Printf("[info] [%s] Skipping %s", msgId, event.String())
	} else {
		log.Printf("[info] [%s] Importing event: %s", msgId, event)
		n, err := Import(ctx, event)
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
	return nil
}
