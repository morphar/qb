package postgres

import (
	"database/sql"
	"log"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

type QueryBase struct {
	db     *sql.DB
	Errors Errors
	stmt   parser.Statement
}

func (q *QueryBase) DB() *sql.DB {
	return q.db
}

func (q *QueryBase) setDB(db *sql.DB) {
	q.db = db
}
