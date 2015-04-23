package rin

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

var (
	DBPool      = make(map[string]*sql.DB, 0)
	DBPoolMutex sync.Mutex
)

func Import(event Event) (int, error) {
	imported := 0
	for _, record := range event.Records {
		for _, target := range config.Targets {
			if target.MatchEventRecord(record) {
				err := ImportRedshift(target, record)
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

func ConnectToRedshift(target Target) (*sql.DB, error) {
	r := target.Redshift
	dsn := r.DSN()

	DBPoolMutex.Lock()
	defer DBPoolMutex.Unlock()

	if db := DBPool[dsn]; db != nil {
		return db, nil
	}
	log.Println("Connect to Redshift", dsn)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	DBPool[dsn] = db
	return db, nil
}

func ImportRedshift(target Target, record EventRecord) error {
	log.Printf("Import to target %s from record %s", target, record)
	db, err := ConnectToRedshift(target)
	if err != nil {
		return err
	}
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	query, err := target.BuildCopySQL(record.S3.Object.Key, config.Credentials)
	if err != nil {
		return err
	}
	if Debug {
		log.Println("SQL:", query)
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
