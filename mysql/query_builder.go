package qb

import (
	"database/sql"
)

type QueryBuilder struct {
	db *sql.DB
}

func New(db *sql.DB) QueryBuilder {
	return QueryBuilder{
		db: db,
	}
}

func (q QueryBuilder) DB() *sql.DB {
	return q.db
}

func (q QueryBuilder) Parse(sql string) (Query, error) {
	query, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	query.setDB(q.db)

	return query, err
}

func (q QueryBuilder) Insert(rows ...interface{}) *InsertQuery {
	i := Insert(rows...)
	i.db = q.db
	return i
}

func (q QueryBuilder) Select(fields ...interface{}) *SelectQuery {
	s := Select(fields...)
	s.db = q.db
	return s
}

func (q QueryBuilder) Update() *UpdateQuery {
	u := Update()
	u.db = q.db
	return u
}

func (q QueryBuilder) Delete() *DeleteQuery {
	d := Delete()
	d.db = q.db
	return d
}
