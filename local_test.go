package rin_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

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
	SQS: &aws.Config{
		Credentials: credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "foo",
				SecretAccessKey: "var",
				SessionToken:    "",
			},
		},
		Region: "ap-northeast-1",
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           SQSEndpoint,
				SigningRegion: "ap-northeast-1",
			}, nil
		}),
	},
	S3: &aws.Config{
		Credentials: credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "foo",
				SecretAccessKey: "var",
				SessionToken:    "",
			},
		},
		Region: "ap-northeast-1",
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           S3Endpoint,
				SigningRegion: "ap-northeast-1",
			}, nil
		}),
	},
	S3OptFns: []func(o *s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = true
		},
	},
}

func TestLocalStack(t *testing.T) {
	if os.Getenv("TEST_LOCALSTACK") == "" {
		return
	}
	d1 := setupSQS(t)
	defer d1()

	d2 := setupS3(t)
	defer d2()

	rin.Sessions = sessions
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rin.RunWithContext(ctx, "s3://rin-test/localstack.yml", &rin.Option{BatchMode: false}); err != nil {
		t.Error(err)
	}
}

func setupS3(t *testing.T) func() {
	svc := s3.NewFromConfig(*sessions.S3, sessions.S3OptFns...)

	bucket := aws.String("rin-test")
	key := aws.String("localstack.yml")

	_, err := svc.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: bucket,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraintApNortheast1,
		},
	})
	if err != nil {
		t.Error(err)
	}

	f, err := os.Open("test/localstack.yml")
	if err != nil {
		t.Error(err)
	}
	_, err = svc.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   f,
	})
	if err != nil {
		t.Error(err)
	}

	return func() {
		svc.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: bucket,
			Key:    key,
		})
		svc.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: bucket,
		})
	}
}

func setupSQS(t *testing.T) func() {
	svc := sqs.NewFromConfig(*sessions.SQS, sessions.SQSOptFns...)
	r, err := svc.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String("rin_test"),
	})
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := svc.SendMessage(context.Background(), &sqs.SendMessageInput{
			MessageBody: aws.String(message),
			QueueUrl:    r.QueueUrl,
		}); err != nil {
			t.Error(err)
		}
	}
	return func() {
		svc.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{
			QueueUrl: r.QueueUrl,
		})
	}
}
