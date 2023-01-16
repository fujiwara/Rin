package rin

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	_ "github.com/lib/pq"
	_ "github.com/mashiike/redshift-data-sql-driver"
)

var (
	DBPool      = make(map[string]*sql.DB, 0)
	DBPoolMutex sync.Mutex
	redshiftSvc *redshift.Client
)

func BoolValue(b *bool) bool {
	if b != nil {
		return *b
	}
	return false
}

func Import(ctx context.Context, event Event) (int, error) {
	var processed int
	for _, record := range event.Records {
	TARGETS:
		for _, target := range config.Targets {
			if ok, cap := target.MatchEventRecord(record); ok {
				if target.Discard {
					processed++
					break TARGETS
				}
				err := target.ImportRedshift(ctx, record, cap)
				if err != nil {
					if BoolValue(config.Redshift.ReconnectOnError) {
						target.DisconnectToRedshift()
					}
					return processed, err
				} else {
					processed++
				}
				if target.Break {
					break TARGETS
				}
			}
		}
	}
	return processed, nil
}

func (target *Target) DisconnectToRedshift() {
	r := target.Redshift
	dsn := r.DSN()
	log.Println("[info] Disconnect to Redshift", r.VisibleDSN())

	DBPoolMutex.Lock()
	defer DBPoolMutex.Unlock()

	if db := DBPool[dsn]; db != nil {
		db.Close()
	}
	delete(DBPool, dsn)
}

func (target *Target) ConnectToRedshift(ctx context.Context) (*sql.DB, error) {
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
	log.Println("[info] Connect to Redshift", r.VisibleDSN())

	var user, password = r.User, r.Password
	// redshift-data driver creates a temporary credentials by itself
	if password == "" && r.Driver != DriverRedshiftData {
		if redshiftSvc == nil {
			redshiftSvc = redshift.NewFromConfig(*Sessions.Redshift, Sessions.RedshiftOptFns...)
		}
		id := strings.SplitN(r.Host, ".", 2)[0]
		log.Printf("[info] Getting cluster credentials for %s user %s", r.Host, r.User)
		res, err := redshiftSvc.GetClusterCredentials(ctx, &redshift.GetClusterCredentialsInput{
			ClusterIdentifier: aws.String(id),
			DbUser:            aws.String(r.User),
		})
		if err != nil {
			return nil, err
		}
		user, password = *res.DbUser, *res.DbPassword
		log.Printf("[debug] Got user %s password %s", user, password)
	}

	db, err := sql.Open(r.Driver, r.DSNWith(user, password))
	if err != nil {
		return nil, err
	}
	DBPool[dsn] = db
	return db, nil
}

func (target *Target) ImportRedshift(ctx context.Context, record *EventRecord, cap *[]string) error {
	if config.Redshift.UseTransaction() {
		return target.importRedshiftWithTx(ctx, record, cap)
	} else {
		return target.importRedshiftWithoutTx(ctx, record, cap)
	}
}

func (target *Target) importRedshiftWithTx(ctx context.Context, record *EventRecord, cap *[]string) error {
	log.Printf("[info] Import to target %s from record %s", target, record)
	db, err := target.ConnectToRedshift(ctx)
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
	log.Println("[debug] SQL:", query)

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

func (target *Target) importRedshiftWithoutTx(ctx context.Context, record *EventRecord, cap *[]string) error {
	log.Printf("[info] Import to target %s from record %s", target, record)
	db, err := target.ConnectToRedshift(ctx)
	if err != nil {
		return err
	}

	query, err := target.BuildCopySQL(record.S3.Object.Key, config.Credentials, cap)
	if err != nil {
		return err
	}
	log.Println("[debug] SQL:", query)

	if _, err := db.ExecContext(ctx, query); err != nil {
		return err
	}

	return nil
}
