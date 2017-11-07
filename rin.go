package rin

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/sqs"
)

var SQS *sqs.SQS
var config *Config
var Debug bool
var Runnable bool
var MaxDeleteRetry = 8
var shutdownBeforeExpiration = 3600 * time.Second

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

func getAuth(config *Config) (*aws.Auth, error) {
	if config.Credentials.AWS_ACCESS_KEY_ID != "" && config.Credentials.AWS_SECRET_ACCESS_KEY != "" {
		return &aws.Auth{
			AccessKey: config.Credentials.AWS_ACCESS_KEY_ID,
			SecretKey: config.Credentials.AWS_SECRET_ACCESS_KEY,
		}, nil
	}
	// Otherwise, use IAM Role
	log.Println("[info] Get instance credentials...")
	cred, err := aws.GetInstanceCredentials()
	if err != nil {
		return nil, err
	}
	exptdate, err := time.Parse("2006-01-02T15:04:05Z", cred.Expiration)
	if err != nil {
		return nil, err
	}
	auth := aws.NewAuth(
		cred.AccessKeyId,
		cred.SecretAccessKey,
		cred.Token,
		exptdate,
	)
	return auth, nil
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

	auth, err := getAuth(config)
	if err != nil {
		return err
	}
	log.Println("[info] access_key_id:", auth.AccessKey)
	region := aws.GetRegion(config.Credentials.AWS_REGION)
	SQS = sqs.New(*auth, region)

	shutdownCh := make(chan interface{})
	exitCh := make(chan int)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, TrapSignals...)

	if !auth.Expiration().IsZero() {
		log.Println("[info] Auth will be expired on", auth.Expiration())
		e := auth.Expiration().Add(-shutdownBeforeExpiration)
		d := e.Sub(time.Now())
		time.AfterFunc(d, func() {
			msg := fmt.Sprintf("Auth will be expired in %s", shutdownBeforeExpiration)
			signalCh <- AuthExpiration{msg}
		})
	}

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
		go func() {
			sqsWorker(shutdownCh)
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
			if _, ok := err.(NoMessageError); ok {
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
			if _, ok := err.(NoMessageError); ok {
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
		return NoMessageError{"No messages"}
	}
	msg := res.Messages[0]
	log.Printf("[info] [%s] Starting process message.", msg.MessageId)
	if Debug {
		log.Printf("[degug] [%s] handle: %s", msg.MessageId, msg.ReceiptHandle)
		log.Printf("[debug] [%s] body: %s", msg.MessageId, msg.Body)
	}
	defer func() {
		if !completed {
			log.Printf("[info] [%s] Aborted message. ReceiptHandle: %s", msg.MessageId, msg.ReceiptHandle)
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
		log.Printf("[warn] [%s] Can't delete message. %s", msg.MessageId, err)
		// retry
		for i := 1; i <= MaxDeleteRetry; i++ {
			log.Printf("[info] [%s] Retry to delete after %d sec.", msg.MessageId, i*i)
			time.Sleep(time.Duration(i*i) * time.Second)
			_, err := queue.DeleteMessage(&msg)
			if err == nil {
				log.Printf("[info] [%s] Message was deleted successfuly.", msg.MessageId)
				break
			}
			log.Printf("[warn] [%s] Can't delete message. %s", msg.MessageId, err)
			if i == MaxDeleteRetry {
				log.Printf("[error] [%s] Max retry count reached. Giving up.", msg.MessageId)
			}
		}
	}

	completed = true
	log.Printf("[info] [%s] Completed message.", msg.MessageId)
	return nil
}
