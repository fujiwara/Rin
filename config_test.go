package rin_test

import (
	"testing"

	rin "github.com/fujiwara/Rin"
)

var BrokenConfig = []string{
	"test/config.yml.invalid_regexp",
	"test/config.yml.no_key_matcher",
	"test/config.yml.not_found",
}

var Expected = [][]string{
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

var ExpectedIAMRole = [][]string{
	[]string{
		"test.bucket.test",
		"test/foo/xxx.json",
		`/* Rin */ COPY "foo" FROM 's3://test.bucket.test/test/foo/xxx.json' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/rin' REGION 'ap-northeast-1' JSON 'auto' GZIP`,
	},
	[]string{
		"test.bucket.test",
		"test/bar/y's.csv",
		`/* Rin */ COPY "xxx"."bar" FROM 's3://test.bucket.test/test/bar/y''s.csv' CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/rin' REGION 'ap-northeast-1' CSV DELIMITER ',' ESCAPE`,
	},
	[]string{
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
	testConfig(t, "test/config.yml", Expected)
	testConfig(t, "test/config.yml.iam_role", ExpectedIAMRole)
}

func testConfig(t *testing.T, name string, expected [][]string) {
	config, err := rin.LoadConfig(name)
	if err != nil {
		t.Fatalf("load config failed: %s", err)
	}
	for _, target := range config.Targets {
		t.Log("target:", target)
	}
	t.Log("global.sql_option", config.SQLOption)
	if len(config.Targets) != 3 {
		t.Error("invalid targets len", len(config.Targets))
	}
	for i, target := range config.Targets {
		e := expected[i]
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
		t.Log(sql)
	}
}
