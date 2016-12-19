# Rin

Rin is a Redshift data Importer by SQS messaging.

## Architecture

1. (Someone) creates a S3 object.
2. [S3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) will send to a message to SQS.
3. Rin will fetch messages from SQS, and publish a "COPY" query to Redshift.

## Configuration

[Configuring Amazon S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html).

1. Create SQS queue.
2. Attach SQS access policy to the queue. [Example Walkthrough 1:](https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html)
3. [Enable Event Notifications](http://docs.aws.amazon.com/AmazonS3/latest/UG/SettingBucketNotifications.html) on a S3 bucket.
4. Run `rin` process with configuration for using the SQS and S3.

### config.yaml

```yaml
queue_name: my_queue_name    # SQS queue name

credentials:
  aws_access_key_id: AAA
  aws_secret_access_key: SSS
  aws_region: ap-northeast-1

redshift:
  host: localhost
  port: 5439
  dbname: test
  user: test_user
  password: test_pass
  schema: public

s3:
  bucket: test.bucket.test
  region: ap-northeast-1

sql_option: "JSON 'auto' GZIP"       # COPY SQL option

# define import target mappings
targets:
  - redshift:
      table: foo
    s3:
      key_prefix: test/foo

  - redshift:
      schema: xxx
      table: bar
    s3:
      key_prefix: test/bar

  - redshift:
      schema: $1      # expand by key_regexp captured value.
      table: $2
    s3:
      key_regexp: test/schema-([a-z]+)/table-([a-z]+)/

  - redshift:
      host: redshift.example.com       # override default section in this target
      port: 5439
      dbname: example
      user: example_user
      password: example_pass
      schema: public
      table: example
    s3:
      bucket: redshift.example.com
      region: ap-northeast-1
      key_prefix: logs/example/
    sql_option: "CSV DELIMITER ',' ESCAPE"
```

#### Credentials

Rin requires credentials for SQS and Redshift.

1. `credentials.aws_access_key_id` and `credentials.aws_secret_access_key`
  - use for SQS and Redshift.
2. `credentials.aws_iam_role`
  - use for Redshift only.
  - for SQS, Rin will try to get a instance credentials.

## Run

### daemon mode

Rin waits new SQS messages and processing it continually.

```
$ rin -config config.yaml [-debug]
```

### batch mode

Rin process new SQS messages and exit.

```
$ rin -config config.yaml -batch [-debug]
```
