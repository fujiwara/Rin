package rin

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

func ParseEvent(b []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(b, &e)
	for _, r := range e.Records {
		if !strings.Contains(r.S3.Object.Key, "%") {
			continue
		}
		if _key, err := url.QueryUnescape(r.S3.Object.Key); err == nil {
			r.S3.Object.Key = _key
		}
	}
	return e, err
}

type Event struct {
	Records []*EventRecord `json:"Records"`
}

func (e Event) String() string {
	s := make([]string, len(e.Records))
	for i, r := range e.Records {
		s[i] = r.String()
	}
	return strings.Join(s, ", ")
}

type EventRecord struct {
	EventVersion string  `json:"eventVersion"`
	EventName    string  `json:"eventName"`
	EventSource  string  `json:"eventSource"`
	EventTime    string  `json:"eventTime"`
	AWSRegion    string  `json:"awsRegion"`
	S3           S3Event `json:"s3"`
}

func (r EventRecord) String() string {
	return r.EventName + " " + fmt.Sprintf(S3URITemplate, r.S3.Bucket.Name, r.S3.Object.Key)
}

type S3Event struct {
	S3SchemaVersion string   `json:"s3SchemaVersion"`
	ConfigurationID string   `json:"configurationId"`
	Bucket          S3Bucket `json:"bucket"`
	Object          S3Object `json:"object"`
}

type S3Bucket struct {
	Name string `json:"name"`
	ARN  string `json:"arn"`
}

type S3Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"eTag"`
}
