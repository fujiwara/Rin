# Rin

Rin is a Redshift data Importer by SQS messaging.

## Configuration

config.yaml

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

## Run

```
$ rin -config config.yaml [-debug]
```
