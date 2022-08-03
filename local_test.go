package rin_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"

	rin "github.com/fujiwara/Rin"
)

const (
	SQSEndpoint      = "http://localhost:4566"
	S3Endpoint       = "http://localhost:4566"
	RedshiftEndpoint = "http://localhost:4566"
)

var message = `{
   "Records":[
      {
         "eventName":"PutObject",
         "s3":{
            "s3SchemaVersion":"1.0",
            "bucket":{
               "name":"rin-test"
            },
            "object":{
               "key":"test/foo/1"
            }
         }
      }
   ]
}`

var sessions = &rin.SessionStore{
	SQS: session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("foo", "var", ""),
		Region:      aws.String(endpoints.ApNortheast1RegionID),
		Endpoint:    aws.String(SQSEndpoint),
	})),
	S3: session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("foo", "var", ""),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(endpoints.ApNortheast1RegionID),
		Endpoint:         aws.String(S3Endpoint),
	})),
}

func TestLocalStackWorker(t *testing.T) {
	if os.Getenv("TEST_LOCALSTACK") == "" {
		return
	}
	d1 := setupSQS(t)
	defer d1()

	d2 := setupS3(t)
	defer d2()

	rin.Sessions = sessions
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	opt := rin.Option{}
	if err := rin.RunWithContext(ctx, "s3://rin-test/localstack.yml", &opt); err != nil {
		t.Error(err)
	}
}

func TestLocalStackBatch(t *testing.T) {
	if os.Getenv("TEST_LOCALSTACK") == "" {
		return
	}
	d1 := setupSQS(t)
	defer d1()

	d2 := setupS3(t)
	defer d2()

	rin.Sessions = sessions
	opt := rin.Option{
		Batch:        true,
		MaxExecCount: 1,
	}
	ctx := context.TODO()
	if err := rin.RunWithContext(ctx, "s3://rin-test/localstack.yml", &opt); err != nil {
		t.Error(err)
	}
}

func setupS3(t *testing.T) func() {
	svc := s3.New(sessions.S3)

	bucket := aws.String("rin-test")
	key := aws.String("localstack.yml")

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: bucket,
	})
	if err != nil {
		t.Error(err)
	}

	f, err := os.Open("test/localstack.yml")
	if err != nil {
		t.Error(err)
	}
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   f,
	})
	if err != nil {
		t.Error(err)
	}

	return func() {
		svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: bucket,
			Key:    key,
		})
		svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: bucket,
		})
	}
}

func setupSQS(t *testing.T) func() {
	svc := sqs.New(sessions.SQS)
	r, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String("rin_test"),
	})
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := svc.SendMessage(&sqs.SendMessageInput{
			MessageBody: aws.String(message),
			QueueUrl:    r.QueueUrl,
		}); err != nil {
			t.Error(err)
		}
	}
	return func() {
		svc.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: r.QueueUrl,
		})
	}
}
