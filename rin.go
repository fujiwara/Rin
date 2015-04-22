package rin

import (
	"log"
	"sync"
	"time"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sqs"
)

var SQS *sqs.SQS
var config *Config

func Run(configFile string) error {
	var err error
	log.Println("Loading config", configFile)
	config, err = LoadConfig(configFile)
	if err != nil {
		return err
	}
	for _, target := range config.Targets {
		log.Println("Loading target", target)
	}

	auth := aws.Auth{
		AccessKey: config.Credentials.AWS_ACCESS_KEY_ID,
		SecretKey: config.Credentials.AWS_SECRET_ACCESS_KEY,
	}
	region := aws.GetRegion(config.Credentials.AWS_REGION)
	SQS = sqs.New(auth, region)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Starting SQS Worker")
		for {
			err := sqsWorker()
			if err != nil {
				log.Println("SQS Worker error:", err)
				time.Sleep(10 * time.Second)
			}
		}
	}()
	wg.Wait()
	return nil
}

func sqsWorker() error {
	log.Println("Connect to SQS:", config.QueueName)
	queue, err := SQS.GetQueue(config.QueueName)
	if err != nil {
		return err
	}
	for {
		res, err := queue.ReceiveMessage(1)
		if err != nil {
			return err
		}
	Messages:
		for _, msg := range res.Messages {
			log.Printf("Message ID:%s", msg.MessageId)
			event, err := ParseEvent([]byte(msg.Body))
			if err != nil {
				log.Println("Can't parse event from Body.", err)
				continue Messages
			}
			log.Println("Importing event:", event)
			n, err := Import(event)
			if err != nil {
				log.Println("Import failed.", err)
				continue Messages
			}
			if n == 0 {
				log.Println("All events were not matched for any targets. Ignored.")
			} else {
				log.Printf("%d import actions completed.")
			}
			_, err = queue.DeleteMessage(&msg)
			if err != nil {
				log.Println("Can't delete message.", err)
				continue Messages
			}
			log.Printf("Completed message ID:%s", msg.MessageId)
		}
		time.Sleep(1 * time.Second)
	}
}
