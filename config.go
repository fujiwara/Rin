package rin

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/lib/pq"
	"gopkg.in/yaml.v1"
)

const (
	S3URITemplate       = "s3://%s/%s"
	CredentialsTemplate = "aws_access_key_id=%s;aws_secret_access_key=%s"
	SQLTemplate         = "/* Rin */ COPY %s FROM %s CREDENTIALS '%s' REGION '%s' %s"
	// Prefix SQL comment "/* Rin */". Because a query which start with "COPY", pq expect a PostgreSQL COPY command response, but a Redshift response is different it.
)

func quoteValue(v string) string {
	return "'" + strings.Replace(v, "'", "''", -1) + "'"
}

type Config struct {
	QueueName   string      `yaml:"queue_name"`
	Targets     []*Target   `yaml:"targets"`
	Credentials Credentials `yaml:"credentials"`
	Redshift    *Redshift   `yaml:"redshift"`
	S3          *S3         `yaml:"s3"`
	SQLOption   string      `yaml:"sql_option"`
}

type Credentials struct {
	AWS_ACCESS_KEY_ID     string `yaml:"aws_access_key_id"`
	AWS_SECRET_ACCESS_KEY string `yaml:"aws_secret_access_key"`
	AWS_REGION            string `yaml:"aws_region"`
}

type Target struct {
	Redshift   *Redshift `yaml:"redshift"`
	S3         *S3       `yaml:"s3"`
	SQLOption  string    `yaml:"sql_option"`
	keyMatcher func(string) (bool, *[]string)
}

type SQLParam struct {
	Table  string
	Option string
}

func (t *Target) String() string {
	return strings.Join([]string{t.S3.String(), t.Redshift.String()}, " => ")
}

func (t *Target) Match(bucket, key string) (bool, *[]string) {
	if bucket != t.S3.Bucket {
		return false, nil
	}
	return t.keyMatcher(key)
}

func (t *Target) MatchEventRecord(r *EventRecord) (bool, *[]string) {
	return t.Match(r.S3.Bucket.Name, r.S3.Object.Key)
}

func (t *Target) buildKeyMatcher() error {
	if prefix := t.S3.KeyPrefix; prefix != "" {
		t.keyMatcher = func(key string) (bool, *[]string) {
			if strings.HasPrefix(key, prefix) {
				cap := []string{key}
				return true, &cap
			} else {
				return false, nil
			}
		}
	} else if r := t.S3.KeyRegexp; r != "" {
		reg, err := regexp.Compile(r)
		if err != nil {
			return err
		}
		t.keyMatcher = func(key string) (bool, *[]string) {
			cap := reg.FindStringSubmatch(key)
			if len(cap) == 0 {
				return false, nil
			} else {
				return true, &cap
			}
		}
	} else {
		return fmt.Errorf("target.key_prefix or key_regexp is not defined")
	}
	return nil
}

func expandPlaceHolder(s string, cap *[]string) string {
	for i, v := range *cap {
		s = strings.Replace(s, "$"+strconv.Itoa(i), v, -1)
	}
	return s
}

func (t *Target) BuildCopySQL(key string, cred Credentials, cap *[]string) (string, error) {
	var table string
	_table := expandPlaceHolder(t.Redshift.Table, cap)
	if t.Redshift.Schema == "" {
		table = pq.QuoteIdentifier(_table)
	} else {
		_schema := expandPlaceHolder(t.Redshift.Schema, cap)
		table = pq.QuoteIdentifier(_schema) + "." + pq.QuoteIdentifier(_table)
	}
	query := fmt.Sprintf(
		SQLTemplate,
		table,
		quoteValue(fmt.Sprintf(S3URITemplate, t.S3.Bucket, key)),
		fmt.Sprintf(CredentialsTemplate, cred.AWS_ACCESS_KEY_ID, cred.AWS_SECRET_ACCESS_KEY),
		t.S3.Region,
		t.SQLOption,
	)
	return query, nil
}

type S3 struct {
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	KeyPrefix string `yaml:"key_prefix"`
	KeyRegexp string `yaml:"key_regexp"`
}

func (s3 S3) String() string {
	if s3.KeyPrefix != "" {
		return fmt.Sprintf(S3URITemplate, s3.Bucket, s3.KeyPrefix)
	} else {
		return fmt.Sprintf(S3URITemplate, s3.Bucket, s3.KeyRegexp)
	}
}

type Redshift struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DBName   string `yaml:"dbname"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
	Table    string `yaml:"table"`
}

func (r Redshift) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		url.QueryEscape(r.User), url.QueryEscape(r.Password),
		url.QueryEscape(r.Host), r.Port, url.QueryEscape(r.DBName),
	)
}

func (r Redshift) VisibleDSN() string {
	return fmt.Sprintf("redshift://%s:****@%s:%d/%s",
		url.QueryEscape(r.User),
		url.QueryEscape(r.Host), r.Port, url.QueryEscape(r.DBName),
	)
}

func (r Redshift) String() string {
	if r.Schema == "" {
		return fmt.Sprintf("%s/public.%s", r.VisibleDSN(), r.Table)
	} else {
		return fmt.Sprintf("%s/%s.%s", r.VisibleDSN(), r.Schema, r.Table)
	}
}

func LoadConfig(path string) (*Config, error) {
	src, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	err = yaml.Unmarshal(src, &c)
	if err != nil {
		return nil, err
	}
	err = (&c).merge()
	if err != nil {
		return nil, err
	}
	return &c, (&c).validate()
}

func (c *Config) validate() error {
	if c.QueueName == "" {
		return fmt.Errorf("queue_name required")
	}
	if len(c.Targets) == 0 {
		return fmt.Errorf("no targets defined")
	}
	return nil
}

func (c *Config) merge() error {
	cr := c.Redshift
	cs := c.S3
	for _, t := range c.Targets {
		if t.SQLOption == "" {
			t.SQLOption = c.SQLOption
		}
		tr := t.Redshift
		if tr == nil {
			t.Redshift = cr
		} else {
			if tr.Host == "" {
				tr.Host = cr.Host
			}
			if tr.Port == 0 {
				tr.Port = cr.Port
			}
			if tr.DBName == "" {
				tr.DBName = cr.DBName
			}
			if tr.User == "" {
				tr.User = cr.User
			}
			if tr.Password == "" {
				tr.Password = cr.Password
			}
			if tr.Schema == "" {
				tr.Schema = cr.Schema
			}
			if tr.Table == "" {
				tr.Table = cr.Table
			}
		}

		ts := t.S3
		if ts == nil {
			t.S3 = cs
		} else {
			if ts.Bucket == "" {
				ts.Bucket = cs.Bucket
			}
			if ts.Region == "" {
				ts.Region = cs.Region
			}
			if ts.KeyPrefix == "" {
				ts.KeyPrefix = cs.KeyPrefix
			}
			if ts.KeyRegexp == "" {
				ts.KeyRegexp = cs.KeyRegexp
			}
		}
		err := t.buildKeyMatcher()
		if err != nil {
			return err
		}
	}
	return nil
}
