package qb

import (
	"database/sql"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

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
