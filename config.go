package rin

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lib/pq"

	goconfig "github.com/kayac/go-config"
)

const (
	S3URITemplate = "s3://%s/%s"
	SQLTemplate   = "/* Rin */ COPY %s FROM %s CREDENTIALS '%s' REGION '%s' %s"
	// Prefix SQL comment "/* Rin */". Because a query which start with "COPY", pq expect a PostgreSQL COPY command response, but a Redshift response is different it.

	DriverPostgres     = "postgres"
	DriverRedshiftData = "redshift-data"
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
	AWS_IAM_ROLE          string `yaml:"aws_iam_role"`
}

func (c Credentials) RedshiftCredential() string {
	if c.AWS_IAM_ROLE != "" {
		return fmt.Sprintf("aws_iam_role=%s", c.AWS_IAM_ROLE)
	} else {
		return fmt.Sprintf("aws_access_key_id=%s;aws_secret_access_key=%s", c.AWS_ACCESS_KEY_ID, c.AWS_SECRET_ACCESS_KEY)
	}
}

type Target struct {
	Redshift  *Redshift `yaml:"redshift"`
	S3        *S3       `yaml:"s3"`
	SQLOption string    `yaml:"sql_option"`
	Break     bool      `yaml:"break"`
	Discard   bool      `yaml:"discard"`

	keyMatcher func(string) (bool, *[]string)
}

type SQLParam struct {
	Table  string
	Option string
}

func (t *Target) String() string {
	var s string
	if t.Discard {
		s = strings.Join([]string{t.S3.String(), "Discard"}, " => ")
	} else {
		s = strings.Join([]string{t.S3.String(), t.Redshift.String()}, " => ")
	}
	if t.Break {
		s = s + " => Break"
	}
	return s
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
				capture := []string{key}
				return true, &capture
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
			capture := reg.FindStringSubmatch(key)
			if len(capture) == 0 {
				return false, nil
			} else {
				return true, &capture
			}
		}
	} else {
		return fmt.Errorf("target.key_prefix or key_regexp is not defined")
	}
	return nil
}

func expandPlaceHolder(s string, capture *[]string) string {
	for i, v := range *capture {
		s = strings.Replace(s, "$"+strconv.Itoa(i), v, -1)
	}
	return s
}

func (t *Target) BuildCopySQL(key string, cred Credentials, capture *[]string) (string, error) {
	var table string
	_table := expandPlaceHolder(t.Redshift.Table, capture)
	if t.Redshift.Schema == "" {
		table = pq.QuoteIdentifier(_table)
	} else {
		_schema := expandPlaceHolder(t.Redshift.Schema, capture)
		table = pq.QuoteIdentifier(_schema) + "." + pq.QuoteIdentifier(_table)
	}
	query := fmt.Sprintf(
		SQLTemplate,
		table,
		quoteValue(fmt.Sprintf(S3URITemplate, t.S3.Bucket, key)),
		cred.RedshiftCredential(),
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
	Driver string `yaml:"driver"`

	// for postgres driver
	Host string `yaml:"host"`
	Port int    `yaml:"port"`

	// for redshift-data driver provisioned
	Cluster string `yaml:"cluster"`

	// for redshift-data driver serverless
	Workgroup string `yaml:"workgroup"`

	DBName           string `yaml:"dbname"`
	User             string `yaml:"user"`
	Password         string `yaml:"password"`
	Schema           string `yaml:"schema"`
	Table            string `yaml:"table"`
	ReconnectOnError *bool  `yaml:"reconnect_on_error"`
}

func (r Redshift) UseTransaction() bool {
	// redshift-data driver does not support transaction
	// https://github.com/mashiike/redshift-data-sql-driver#unsupported-features
	return r.Driver == DriverPostgres
}

func (r Redshift) DSN() string {
	return r.DSNWith(r.User, r.Password)
}

func (r Redshift) DSNWith(user string, password string) string {
	switch r.Driver {
	case DriverPostgres:
		var p string
		if password != "" {
			p = url.QueryEscape(password)
		} else {
			p = "****"
		}
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			url.QueryEscape(user), p,
			url.QueryEscape(r.Host), r.Port, url.QueryEscape(r.DBName),
		)
	case DriverRedshiftData:
		if r.Workgroup != "" {
			return fmt.Sprintf("workgroup(%s)/%s",
				url.QueryEscape(r.Workgroup),
				url.QueryEscape(r.DBName),
			)
		} else if r.Cluster != "" {
			return fmt.Sprintf("%s@cluster(%s)/%s",
				url.QueryEscape(user),
				url.QueryEscape(r.Cluster),
				url.QueryEscape(r.DBName),
			)
		}
		panic("redshift-data driver requires workgroup or cluster")
	default:
		panic("unknown driver: " + r.Driver)
	}
}

func (r Redshift) VisibleDSN() string {
	if r.Driver == DriverPostgres {
		return strings.Replace(r.DSNWith(r.User, ""), "postgres://", "redshift://", 1)
	} else {
		return r.Driver + "://" + r.DSN()
	}
}

func (r Redshift) String() string {
	if r.Schema == "" {
		return fmt.Sprintf("%s/public.%s", r.VisibleDSN(), r.Table)
	} else {
		return fmt.Sprintf("%s/%s.%s", r.VisibleDSN(), r.Schema, r.Table)
	}
}

func loadSrcFrom(ctx context.Context, path string) ([]byte, error) {
	u, err := url.Parse(path)
	if err != nil {
		// not a URL. load as a file path
		return ioutil.ReadFile(path)
	}
	switch u.Scheme {
	case "http", "https":
		return fetchHTTP(ctx, u)
	case "s3":
		return fetchS3(ctx, u)
	case "file", "":
		return ioutil.ReadFile(u.Path)
	default:
		return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
	}
}

func fetchHTTP(ctx context.Context, u *url.URL) ([]byte, error) {
	log.Println("[info] fetching from", u)
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func fetchS3(ctx context.Context, u *url.URL) ([]byte, error) {
	log.Println("[info] fetching from", u)
	var s3Svc *s3.Client
	if Sessions.S3 == nil {
		awsCfg, err := awsConfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load default aws config, %w", err)
		}
		s3Svc = s3.NewFromConfig(awsCfg)
	} else {
		s3Svc = s3.NewFromConfig(*Sessions.S3, Sessions.S3OptFns...)
	}
	bucket := u.Host
	key := strings.TrimLeft(u.Path, "/")
	headObject, err := s3Svc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to head object from S3, %w", err)
	}
	buf := make([]byte, int(headObject.ContentLength))
	w := manager.NewWriteAtBuffer(buf)
	downloader := manager.NewDownloader(s3Svc)
	_, err = downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from S3, %s", err)
	}
	return buf, nil
}

func LoadConfig(ctx context.Context, path string) (*Config, error) {
	src, err := loadSrcFrom(ctx, path)
	if err != nil {
		return nil, err
	}
	var c Config = Config{
		Redshift: &Redshift{
			Driver: DriverPostgres, // default
		},
	}
	err = goconfig.LoadWithEnvBytes(&c, src)
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
	switch c.Redshift.Driver {
	case DriverPostgres, DriverRedshiftData: // ok
		log.Println("[debug] redshift.driver is", c.Redshift.Driver)
	default:
		return fmt.Errorf("invalid redshift.driver must be %s or %s", DriverPostgres, DriverRedshiftData)
	}
	if c.QueueName == "" {
		if !isLambda() {
			return fmt.Errorf("queue_name required")
		}
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
			if tr.ReconnectOnError == nil {
				tr.ReconnectOnError = cr.ReconnectOnError
			}
			if tr.Driver == "" {
				tr.Driver = cr.Driver
			}
			if tr.Workgroup == "" {
				tr.Workgroup = cr.Workgroup
			}
			if tr.Cluster == "" {
				tr.Cluster = cr.Cluster
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
