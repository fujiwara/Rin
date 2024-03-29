package rin_test

import (
	"context"
	"os"
	"testing"

	rin "github.com/fujiwara/Rin"
)

var BrokenConfig = []string{
	"test/config.yml.invalid_regexp",
	"test/config.yml.no_key_matcher",
	"test/config.yml.not_found",
}

type testExpected struct {
	targets    []testTarget
	Driver     string
	DSN        string
	VisibleDSN string
}

type testTarget struct {
	Bucket string
	Key    string
	SQL    string
}

var Expected = testExpected{
	DSN:        "postgres://test_user:test_pass@localhost:5432/test",
	VisibleDSN: "redshift://test_user:****@localhost:5432/test",
	Driver:     "postgres",
	targets: []testTarget{
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
	},
}

var ExpectedIAMRole = testExpected{
	DSN:        "postgres://test_user:test_pass@localhost:5432/test",
	VisibleDSN: "redshift://test_user:****@localhost:5432/test",
	Driver:     "postgres",
	targets: []testTarget{
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
	},
}

var ExpectedRedshiftData = testExpected{
	DSN:        "test_user@cluster(mycluster)/test",
	VisibleDSN: "redshift-data://test_user@cluster(mycluster)/test",
	Driver:     "redshift-data",
	targets: []testTarget{
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
	},
}

func TestLoadConfigError(t *testing.T) {
	ctx := context.Background()
	for _, f := range BrokenConfig {
		_, err := rin.LoadConfig(ctx, f)
		if err == nil {
			t.Errorf("LoadConfig(%s) must be failed", f)
		}
		t.Log(err)
	}
}

func TestLoadConfig(t *testing.T) {
	os.Setenv("AWS_SECRET_ACCESS_KEY", "SSS")
	testConfig(t, "test/config.yml", Expected)
	testConfig(t, "test/config.iam_role.yml", ExpectedIAMRole)
	testConfig(t, "test/config.redshift-data.yml", ExpectedRedshiftData)
}

func testConfig(t *testing.T, name string, expected testExpected) {
	ctx := context.Background()
	config, err := rin.LoadConfig(ctx, name)
	if err != nil {
		t.Fatalf("load config failed: %s", err)
	}
	//t.Log("global.sql_option", config.SQLOption)
	if len(config.Targets) != 5 {
		t.Error("invalid targets len", len(config.Targets))
	}

	if expected.DSN != config.Redshift.DSN() {
		t.Errorf("invalid DSN expected %s got %s", expected.DSN, config.Redshift.DSN())
	}
	if expected.VisibleDSN != config.Redshift.VisibleDSN() {
		t.Errorf("invalid VisibleDSN expected %s got %s", expected.VisibleDSN, config.Redshift.VisibleDSN())
	}
	if expected.Driver != config.Redshift.Driver {
		t.Errorf("invalid Driver expected %s got %s", expected.Driver, config.Redshift.Driver)
	}

	for _, e := range expected.targets {
		var sql string
		var err error
		for i, target := range config.Targets {
			ok, cap := target.Match(e.Bucket, e.Key)
			if !ok {
				continue
			}
			if target.Discard {
				t.Log("discard", e.Key, "target", i)
				break
			} else {
				t.Log("build", e.Key, "target", i)
				sql, err = target.BuildCopySQL(e.Key, config.Credentials, cap)
				if err != nil {
					t.Error(err)
				}
				//t.Log(sql)
			}
			if target.Break {
				t.Log("break", e.Key, "target", i)
				break
			}
			if !rin.BoolValue(target.Redshift.ReconnectOnError) {
				t.Error("reconnect_on_error must be true")
			}
		}
		if sql != e.SQL {
			t.Errorf("unexpected SQL:\nExpected:%s\nGot:%s", e.SQL, sql)
		}
	}
}
