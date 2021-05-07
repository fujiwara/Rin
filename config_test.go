package rin_test

import (
	"os"
	"testing"

	rin "github.com/fujiwara/Rin"
)

var BrokenConfig = []string{
	"test/config.yml.invalid_regexp",
	"test/config.yml.no_key_matcher",
	"test/config.yml.not_found",
}

var Expected = [][]string{
	{
		"test.bucket.test",
		"test/foo/xxx.json",
		`/* Rin */ COPY "foo" FROM 's3://test.bucket.test/test/foo/xxx.json' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
	{
		"test.bucket.test",
		"test/foo/discard/xxx.json",
		"",
	},
	{
		"test.bucket.test",
		"test/bar/break",
		`/* Rin */ COPY "xxx"."bar_break" FROM 's3://test.bucket.test/test/bar/break' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
	{
		"test.bucket.test",
		"test/bar/y's.csv",
		`/* Rin */ COPY "xxx"."bar" FROM 's3://test.bucket.test/test/bar/y''s.csv' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
	{
		"example.bucket",
		"test/s1/t256/aaa.json",
		`/* Rin */ COPY "s1"."t256" FROM 's3://example.bucket/test/s1/t256/aaa.json' CREDENTIALS 'aws_access_key_id=AAA;aws_secret_access_key=SSS' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
}

var ExpectedIAMRole = [][]string{
	{
		"test.bucket.test",
		"test/foo/xxx.json",
		`/* Rin */ COPY "foo" FROM 's3://test.bucket.test/test/foo/xxx.json' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/rin' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
	{
		"test.bucket.test",
		"test/foo/discard/xxx.json",
		"",
	},
	{
		"test.bucket.test",
		"test/bar/break",
		`/* Rin */ COPY "xxx"."bar_break" FROM 's3://test.bucket.test/test/bar/break' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/rin' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
	{
		"test.bucket.test",
		"test/bar/y's.csv",
		`/* Rin */ COPY "xxx"."bar" FROM 's3://test.bucket.test/test/bar/y''s.csv' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/rin' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
	{
		"example.bucket",
		"test/s1/t256/aaa.json",
		`/* Rin */ COPY "s1"."t256" FROM 's3://example.bucket/test/s1/t256/aaa.json' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/rin' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
}

func TestLoadConfigError(t *testing.T) {
	for _, f := range BrokenConfig {
		_, err := rin.LoadConfig(f)
		if err == nil {
			t.Errorf("LoadConfig(%s) must be failed", f)
		}
		t.Log(err)
	}
}

func TestLoadConfig(t *testing.T) {
	os.Setenv("AWS_SECRET_ACCESS_KEY", "SSS")
	testConfig(t, "test/config.yml", Expected)
	testConfig(t, "test/config.yml.iam_role", ExpectedIAMRole)
}

func testConfig(t *testing.T, name string, expected [][]string) {
	config, err := rin.LoadConfig(name)
	if err != nil {
		t.Fatalf("load config failed: %s", err)
	}
	//t.Log("global.sql_option", config.SQLOption)
	if len(config.Targets) != 5 {
		t.Error("invalid targets len", len(config.Targets))
	}
	for _, e := range expected {
		bucket := e[0]
		key := e[1]
		var sql string
		var err error
		for i, target := range config.Targets {
			ok, cap := target.Match(bucket, key)
			if !ok {
				continue
			}
			if target.Discard {
				t.Log("discard", key, "target", i)
				break
			} else {
				t.Log("build", key, "target", i)
				sql, err = target.BuildCopySQL(key, config.Credentials, cap)
				if err != nil {
					t.Error(err)
				}
				//t.Log(sql)
			}
			if target.Break {
				t.Log("break", key, "target", i)
				break
			}
			if !target.Redshift.ReconnectOnError {
				t.Error("reconnect_on_error must be true")
			}
		}
		if sql != e[2] {
			t.Errorf("unexpected SQL:\nExpected:%s\nGot:%s", e[2], sql)
		}
	}
}
