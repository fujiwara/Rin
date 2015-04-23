package rin_test

import (
	"log"
	"testing"

	rin "github.com/fujiwara/Rin"
)

var Excepted = [][]string{
	[]string{
		"test.bucket.test",
		"test/foo/xxx.json",
		`COPY "foo" FROM 's3://test.bucket.test/test/foo/xxx.json' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
	[]string{
		"test.bucket.test",
		"test/bar/y's.csv",
		`COPY "xxx"."bar" FROM 's3://test.bucket.test/test/bar/y''s.csv' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
}

func TestLoadConfig(t *testing.T) {
	config, err := rin.LoadConfig("test/config.yml")
	if err != nil {
		t.Errorf("load config failed: %s", err)
	}
	if len(config.Targets) != 2 {
		t.Error("invalid targets len", len(config.Targets))
	}
	for i, target := range config.Targets {
		e := Excepted[i]
		bucket := e[0]
		key := e[1]
		if !target.Match(bucket, key) {
			t.Errorf("%s %s is not match target: %s", bucket, key, target)
		}
		sql, err := target.BuildCopySQL(key, config.Credentials)
		if err != nil {
			t.Error(err)
		}
		if sql != e[2] {
			t.Errorf("unexpected SQL: %s", sql)
		}
		log.Println(sql)
	}
}
