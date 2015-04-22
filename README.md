# Rin

Rin is a Redshift data Importer by SQS messaging.

## Configuration

config.yaml

```yaml
queue_name: my_queue_name
credentials:
  aws_access_key_id: AAA
  aws_secret_access_key: SSS
  aws_region: ap-northeast-1
targets:
  - redshift:
      host: localhost
      port: 5432
      dbname: test
      user: test_user
      password: test_pass
      table: foo
    s3:
      bucket: test.bucket.test
      region: ap-northeast-1
      key_prefix: test/foo
    sql_option: "JSON GZIP"
  - redshift:
      host: localhost
      port: 5432
      dbname: test
      user: test_user
      password: test_pass
      table: bar
    s3:
      bucket: test.bucket.test
      region: ap-northeast-1
      key_prefix: test/bar
    sql_option: "CSV DELIMITER ',' ESCAPE"
```

## Run

```
$ rin -config config.yaml
```
