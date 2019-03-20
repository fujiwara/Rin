package rin

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var config *Config
var Debug bool
var Runnable bool
var MaxDeleteRetry = 8
var shutdownBeforeExpiration = 3600 * time.Second
var Session *session.Session

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

type AuthExpiration struct {
	s string
}

func (e AuthExpiration) Error() string {
	return e.s
}

func (e AuthExpiration) String() string {
	return e.s
}

func (e AuthExpiration) Signal() {
}

func Run(configFile string, batchMode bool) error {
	Runnable = true
	var err error
	log.Println("[info] Loading config:", configFile)
	config, err = LoadConfig(configFile)
	if err != nil {
		return err
	}
	for _, target := range config.Targets {
		log.Println("[info] Define target", target.String())
	}

	if Session == nil {
		Session = session.Must(session.NewSession())
	}
	sqsSvc := sqs.New(Session, aws.NewConfig().WithRegion(config.Credentials.AWS_REGION))

	shutdownCh := make(chan interface{})
	exitCh := make(chan int)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, TrapSignals...)

	// run worker
	if batchMode {
		go func() {
			err := sqsBatch(shutdownCh, sqsSvc)
			if err != nil {
				log.Println("[error]", err)
				exitCh <- 1
			}
			exitCh <- 0
		}()
	} else {
		go func() {
			sqsWorker(shutdownCh, sqsSvc)
			exitCh <- 0
		}()
	}

	// wait for signal
	var exitCode = 0
	var exitErr error
	select {
	case s := <-signalCh:
		switch sig := s.(type) {
		case syscall.Signal:
			log.Printf("[info] Got signal: %s(%d)", sig, sig)
		case AuthExpiration:
			log.Printf("[info] %s", sig)
			exitErr = sig
		}
		log.Println("[info] Shutting down worker...")
		close(shutdownCh)   // notify shutdown to worker
		exitCode = <-exitCh // wait for shutdown worker
	case exitCode = <-exitCh:
	}

	log.Println("[info] Shutdown.")
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return exitErr
}

func waitForRetry() {
	log.Println("[warn] Retry after 10 sec.")
	time.Sleep(10 * time.Second)
}

func runnable(ch chan interface{}) bool {
	if !Runnable {
		return false
	}
	select {
	case <-ch:
		// ch closed == shutdown
		Runnable = false
		return false
	default:
	}
	return true
}

func sqsBatch(ch chan interface{}, svc *sqs.SQS) error {
	log.Printf("[info] Starting up SQS Batch")
	defer log.Println("[info] Shutdown SQS Batch")

	log.Println("[info] Connect to SQS:", config.QueueName)
	res, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(config.QueueName),
	})
	if err != nil {
		return err
	}
	for runnable(ch) {
		err := handleMessage(svc, res.QueueUrl)
		if err != nil {
			if _, ok := err.(NoMessageError); ok {
				break
			} else {
				return err
			}
		}
	}
	return nil
}

func sqsWorker(ch chan interface{}, svc *sqs.SQS) {
	log.Printf("[info] Starting up SQS Worker")
	defer log.Println("[info] Shutdown SQS Worker")

	for runnable(ch) {
		log.Println("[info] Connect to SQS:", config.QueueName)
		res, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(config.QueueName),
		})
		if err != nil {
			log.Println("[error] GetQueueUrl failed:", err)
			waitForRetry()
			continue
		}
		quit, err := handleQueue(ch, svc, res.QueueUrl)
		if err != nil {
			log.Println("[error] Processing failed:", err)
			waitForRetry()
			continue
		}
		if quit {
			break
		}
	}
}

func handleQueue(ch chan interface{}, svc *sqs.SQS, queueUrl *string) (bool, error) {
	for runnable(ch) {
		err := handleMessage(svc, queueUrl)
		if err != nil {
			if _, ok := err.(NoMessageError); ok {
				continue
			} else {
				return false, err
			}
		}
	}
	return true, nil
}

func handleMessage(svc *sqs.SQS, queueUrl *string) error {
	var completed = false
	res, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
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
	if Debug {
		log.Printf("[degug] [%s] handle: %s", msgId, *msg.ReceiptHandle)
		log.Printf("[debug] [%s] body: %s", msgId, *msg.Body)
	}
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
	log.Printf("[info] [%s] Importing event: %s", msgId, event)
	n, err := Import(event)
	if err != nil {
		log.Printf("[error] [%s] Import failed. %s", msgId, err)
		return err
	}
	if n == 0 {
		log.Printf("[warn] [%s] All events were not matched for any targets. Ignored.", msgId)
	} else {
		log.Printf("[info] [%s] %d import action completed.", msgId, n)
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
