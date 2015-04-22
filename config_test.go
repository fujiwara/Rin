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
		"COPY foo FROM $1 CREDENTIALS $2 REGION $3 JSON GZIP",
		"s3://test.bucket.test/test/foo/xxx.json",
		"aws_access_key_id=AAA;aws_secret_access_key=SSS",
		"ap-northeast-1",
	},
	[]string{
		"test.bucket.test",
		"test/bar/yyy.csv",
		"COPY bar FROM $1 CREDENTIALS $2 REGION $3 CSV DELIMITER ',' ESCAPE",
		"s3://test.bucket.test/test/bar/yyy.csv",
		"aws_access_key_id=AAA;aws_secret_access_key=SSS",
		"ap-northeast-1",
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
		sql, binds, err := target.BuildCopySQL(key, config.Credentials)
		if err != nil {
			t.Error(err)
		}
		if sql != e[2] {
			t.Errorf("unexpected SQL: %s", sql)
		}
		if len(binds) != 3 {
			t.Errorf("unexpected bind params: %#v", binds)
		}
		if binds[0].(string) != e[3] {
			t.Errorf("unexpected bind param: %s", binds[0])
		}
		if binds[1].(string) != e[4] {
			t.Errorf("unexpected bind param: %s", binds[1])
		}
		if binds[2].(string) != e[5] {
			t.Errorf("unexpected bind param: %s", binds[2])
		}
		log.Println(sql, binds)
	}
}
