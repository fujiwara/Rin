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
		`/* Rin */ COPY "foo" FROM 's3://test.bucket.test/test/foo/xxx.json' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
	[]string{
		"test.bucket.test",
		"test/bar/y's.csv",
		`/* Rin */ COPY "xxx"."bar" FROM 's3://test.bucket.test/test/bar/y''s.csv' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
	[]string{
		"example.bucket",
		"test/s1/t256/aaa.json",
		`/* Rin */ COPY "s1"."t256" FROM 's3://example.bucket/test/s1/t256/aaa.json' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
}

func TestLoadConfig(t *testing.T) {
	config, err := rin.LoadConfig("test/config.yml")
	for _, t := range config.Targets {
		log.Println("target:", t)
	}
	log.Println("global.sql_option", config.SQLOption)
	if err != nil {
		t.Errorf("load config failed: %s", err)
	}
	if len(config.Targets) != 3 {
		t.Error("invalid targets len", len(config.Targets))
	}
	for i, target := range config.Targets {
		e := Excepted[i]
		bucket := e[0]
		key := e[1]
		ok, cap := target.Match(bucket, key)
		if !ok {
			t.Errorf("%s %s is not match target: %s", bucket, key, target)
		}
		sql, err := target.BuildCopySQL(key, config.Credentials, cap)
		if err != nil {
			t.Error(err)
		}
		if sql != e[2] {
			t.Errorf("unexpected SQL:\n%s\n%s", sql, e[2])
		}
		log.Println(sql)
	}
}
