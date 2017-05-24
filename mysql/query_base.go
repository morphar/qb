package qb

import (
	"database/sql"

	parser "github.com/morphar/sqlparsers/mysql"
)

type QueryBase struct {
	db     *sql.DB
	Errors Errors
	Stmt   parser.Statement
}

func (q *QueryBase) DB() *sql.DB {
	return q.db
}

func (q *QueryBase) setDB(db *sql.DB) {
	q.db = db
}
