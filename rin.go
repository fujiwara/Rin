package rin

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

var SQS *sqs.SQS
var config *Config
var Debug bool
var Runnable bool

var TrapSignals = []os.Signal{
	syscall.SIGHUP,
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGQUIT,
}

type NoMessageError struct {
	s string
}

func (e *NoMessageError) Error() string {
	return e.s
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
		log.Println("[info] Define target", target)
	}

	auth := aws.Auth{
		AccessKey: config.Credentials.AWS_ACCESS_KEY_ID,
		SecretKey: config.Credentials.AWS_SECRET_ACCESS_KEY,
	}
	region := aws.GetRegion(config.Credentials.AWS_REGION)
	SQS = sqs.New(auth, region)

	shutdownCh := make(chan interface{})
	exitCh := make(chan int)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, TrapSignals...)

	// run worker
	if batchMode {
		go func() {
			err := sqsBatch(shutdownCh)
			if err != nil {
				log.Println("[error]", err)
				exitCh <- 1
			}
			exitCh <- 0
		}()
	} else {
		go sqsWorker(shutdownCh)
	}

	// wait for signal
	var exitCode int
	select {
	case s := <-signalCh:
		switch sig := s.(type) {
		case syscall.Signal:
			log.Printf("[info] Got signal: %s(%d)", sig, sig)
		default:
		}
		log.Println("[info] Shutting down worker...")
		close(shutdownCh) // notify shutdown to worker
	case exitCode = <-exitCh:
	}

	log.Println("[info] Shutdown.")
	os.Exit(exitCode)
	return nil
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

func sqsBatch(ch chan interface{}) error {
	log.Printf("[info] Starting up SQS Batch")
	defer log.Println("[info] Shutdown SQS Batch")

	log.Println("[info] Connect to SQS:", config.QueueName)
	queue, err := SQS.GetQueue(config.QueueName)
	if err != nil {
		return err
	}
	for runnable(ch) {
		err := handleMessage(queue)
		if err != nil {
			if _, ok := err.(*NoMessageError); ok {
				break
			} else {
				return err
			}
		}
	}
	return nil
}

func sqsWorker(ch chan interface{}) {
	log.Printf("[info] Starting up SQS Worker")
	defer log.Println("[info] Shutdown SQS Worker")

	for runnable(ch) {
		log.Println("[info] Connect to SQS:", config.QueueName)
		queue, err := SQS.GetQueue(config.QueueName)
		if err != nil {
			log.Println("[error] Can't get queue:", err)
			waitForRetry()
			continue
		}
		quit, err := handleQueue(queue, ch)
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

func handleQueue(queue *sqs.Queue, ch chan interface{}) (bool, error) {
	for runnable(ch) {
		err := handleMessage(queue)
		if err != nil {
			if _, ok := err.(*NoMessageError); ok {
				continue
			} else {
				return false, err
			}
		}
	}
	return true, nil
}

func handleMessage(queue *sqs.Queue) error {
	var completed = false
	res, err := queue.ReceiveMessage(1)
	if err != nil {
		return err
	}
	if len(res.Messages) == 0 {
		return &NoMessageError{"No messages"}
	}
	msg := res.Messages[0]
	log.Printf("[info] [%s] Starting process message.", msg.MessageId)
	if Debug {
		log.Printf("[degug] [%s] handle: %s", msg.MessageId, msg.ReceiptHandle)
		log.Printf("[debug] [%s] body: %s", msg.MessageId, msg.Body)
	}
	defer func() {
		if !completed {
			log.Printf("[info] [%s] Aborted message.", msg.MessageId)
		}
	}()

	event, err := ParseEvent([]byte(msg.Body))
	if err != nil {
		log.Printf("[error] [%s] Can't parse event from Body.", msg.MessageId, err)
		return err
	}
	log.Printf("[info] [%s] Importing event: %s", msg.MessageId, event)
	n, err := Import(event)
	if err != nil {
		log.Printf("[error] [%s] Import failed. %s", msg.MessageId, err)
		return err
	}
	if n == 0 {
		log.Printf("[warn] [%s] All events were not matched for any targets. Ignored.", msg.MessageId)
	} else {
		log.Printf("[info] [%s] %d import action completed.", msg.MessageId, n)
	}
	_, err = queue.DeleteMessage(&msg)
	if err != nil {
		log.Printf("[error] [%s] Can't delete message. %s", msg.MessageId, err)
	}
	completed = true
	log.Printf("[info] [%s] Completed message.", msg.MessageId)
	return nil
}
