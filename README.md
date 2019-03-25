# Rin

Rin is a Redshift data Importer by SQS messaging.

## Architecture

1. (Someone) creates a S3 object.
2. [S3 event notifications](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) will send to a message to SQS.
3. Rin will fetch messages from SQS, and publish a "COPY" query to Redshift.

## Installation

### Binary packages

[Releases](https://github.com/fujiwara/Rin/releases)

### Homebrew

```console
$ brew install fujiwara/tap/rin
```

### Docker

[DockerHub](https://cloud.docker.com/u/fujiwara/repository/docker/fujiwara/rin)

```console
$ docker pull fujiwara/rin:latest
```

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
  aws_region: ap-northeast-1

redshift:
  host: localhost
  port: 5439
  dbname: test
  user: test_user
  password: '{{ must_env "REDSHIFT_PASSWORD" }}'
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

A configuration file is parsed by [kayac/go-config](https://github.com/kayac/go-config).

go-config expands environment variables using syntax `{{ env "FOO" }}` or `{{ must_env "FOO" }}` in a configuration file.

When the password for Redshift is empty, Rin will try call [GetClusterCredentials API](https://docs.aws.amazon.com/redshift/latest/APIReference/API_GetClusterCredentials.html) to get a temporary password for the cluster.

#### Credentials

Rin requires credentials for SQS and Redshift.

1. `credentials.aws_access_key_id` and `credentials.aws_secret_access_key`
  - used for SQS and Redshift.
2. `credentials.aws_iam_role`
  - used for Redshift only.
  - for SQS, Rin will try to get a instance credentials.

## Run

### daemon mode

Rin waits new SQS messages and processing it continually.

```
$ rin -config config.yaml [-debug]
```

`-config` also accepts HTTP/S3/File URL to specify the location of configuration file.
For example,

```
$ rin -config s3://rin-config.my-bucket/config.yaml
```

### batch mode

Rin process new SQS messages and exit.

```
$ rin -config config.yaml -batch [-debug]
```
