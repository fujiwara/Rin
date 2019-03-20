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
	"github.com/aws/aws-sdk-go/service/sqs"

	rin "github.com/fujiwara/Rin"
)

const SQSEndpoint = "http://localhost:4576"

var message = `{
   "Records":[
      {
         "eventName":"PutObject",
         "s3":{
            "s3SchemaVersion":"1.0",
            "bucket":{
               "name":"xxx.yyy.zzz"
            },
            "object":{
               "key":"foo/bar"
            }
         }
      }
   ]
}`

var sess = session.Must(session.NewSession(&aws.Config{
	Credentials:      credentials.NewStaticCredentials("foo", "var", ""),
	S3ForcePathStyle: aws.Bool(true),
	Region:           aws.String(endpoints.UsWest2RegionID),
	Endpoint:         aws.String(SQSEndpoint),
}))

func TestLocalStack(t *testing.T) {
	if os.Getenv("TEST_LOCALSTACK") == "" {
		return
	}

	svc := sqs.New(sess)
	r, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String("rin_test"),
	})
	if err != nil {
		t.Error(err)
	}
	defer svc.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: r.QueueUrl})

	for i := 0; i < 2; i++ {
		if _, err := svc.SendMessage(&sqs.SendMessageInput{
			MessageBody: aws.String(message),
			QueueUrl:    r.QueueUrl,
		}); err != nil {
			t.Error(err)
		}
	}

	rin.Session = sess
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rin.RunWithContext(ctx, "test/localstack.yml", false); err != nil {
		t.Error(err)
	}
}
