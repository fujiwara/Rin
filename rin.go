package rin

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/sns"
)

var SNS *sns.SNS
var config *Config

func Run(configFile string, port int) error {
	var err error
	log.Println("Loading config", configFile)
	config, err = LoadConfig(configFile)
	if err != nil {
		return err
	}

	auth := aws.Auth{
		AccessKey: config.Credentials.AWS_ACCESS_KEY_ID,
		SecretKey: config.Credentials.AWS_SECRET_ACCESS_KEY,
	}
	region := aws.GetRegion(config.Credentials.AWS_REGION)
	SNS, err = sns.New(auth, region)
	if err != nil {
		return err
	}

	http.HandleFunc("/", snsHandler)
	addr := fmt.Sprintf(":%d", port)
	log.Println("Listening", addr)

	log.Fatal(http.ListenAndServe(addr, nil))
	return nil
}

func serverError(w http.ResponseWriter, code int) {
	if code == 0 {
		code = http.StatusInternalServerError
	}
	http.Error(w, http.StatusText(code), code)
}

func snsHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		serverError(w, http.StatusMethodNotAllowed)
		return
	}
	var n *sns.HttpNotification
	dec := json.NewDecoder(req.Body)
	dec.Decode(&n)
	log.Println("sns", n.Type, n.TopicArn, n.Subject)
	switch n.Type {
	case "SubscriptionConfirmation":
		_, err := SNS.ConfirmSubscriptionFromHttp(n, "no")
		if err != nil {
			log.Println(err)
			serverError(w, http.StatusInternalServerError)
			return
		}
	case "Nortification":
		event, err := ParseEvent([]byte(n.Message))
		if err != nil {
			log.Println("Can't parse event string", n.Message, err)
			serverError(w, http.StatusInternalServerError)
			return
		}
		err = Import(event)
		if err != nil {
			serverError(w, http.StatusInternalServerError)
			return
		}
	}
	io.WriteString(w, "OK")
}
