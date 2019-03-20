package rin

import (
	"database/sql"
	"log"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/redshift"
	_ "github.com/lib/pq"
)

var (
	DBPool      = make(map[string]*sql.DB, 0)
	DBPoolMutex sync.Mutex
	redshiftSvc *redshift.Redshift
)

func Import(event Event) (int, error) {
	imported := 0
	for _, record := range event.Records {
		for _, target := range config.Targets {
			if ok, cap := target.MatchEventRecord(record); ok {
				err := ImportRedshift(target, record, cap)
				if err != nil {
					return imported, err
				} else {
					imported++
				}
			}
		}
	}
	return imported, nil
}

func ConnectToRedshift(target *Target) (*sql.DB, error) {
	r := target.Redshift
	dsn := r.DSN()

	DBPoolMutex.Lock()
	defer DBPoolMutex.Unlock()

	if db := DBPool[dsn]; db != nil {
		if db.Ping() == nil {
			return db, nil
		} else {
			delete(DBPool, dsn)
		}
	}
	log.Println("Connect to Redshift", dsn)

	var password = r.Password
	if password == "" {
		if redshiftSvc == nil {
			redshiftSvc = redshift.New(Sessions.Redshift)
		}
		id := strings.SplitN(r.Host, ".", 2)[0]
		log.Printf("[info] Getting cluster credentials for %s user %s", r.Host, r.User)
		res, err := redshiftSvc.GetClusterCredentials(&redshift.GetClusterCredentialsInput{
			ClusterIdentifier: aws.String(id),
			DbUser:            aws.String(r.User),
		})
		if err != nil {
			return nil, err
		}
		password = *res.DbPassword
		log.Printf("[debug] Got temp password %s", password)
	}

	db, err := sql.Open("postgres", r.DSNWithPassword(password))
	if err != nil {
		return nil, err
	}
	DBPool[dsn] = db
	return db, nil
}

func ImportRedshift(target *Target, record *EventRecord, cap *[]string) error {
	log.Printf("[info] Import to target %s from record %s", target, record)
	db, err := ConnectToRedshift(target)
	if err != nil {
		return err
	}
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	query, err := target.BuildCopySQL(record.S3.Object.Key, config.Credentials, cap)
	if err != nil {
		return err
	}
	if Debug {
		log.Println("[debug] SQL:", query)
	}
	stmt, err := txn.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}
	return nil
}
