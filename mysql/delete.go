package qb

import (
	"errors"

	parser "github.com/morphar/sqlparsers/mysql"
)

type DeleteQuery struct {
	QueryBase

	Stmt *parser.Delete
}

func (q *DeleteQuery) From(froms ...interface{}) *DeleteQuery {
	q.Stmt.TableExprs = parser.TableExprs{}

	for _, from := range froms {
		q.Stmt.TableExprs = append(q.Stmt.TableExprs, T(from))
	}

	return q
}

func (q *DeleteQuery) Where(wheres ...parser.Expr) *DeleteQuery {
	if q.Stmt.Where == nil {
		q.Stmt.Where = &parser.Where{Type: "where"}
	}

	for _, where := range wheres {
		if q.Stmt.Where.Expr == nil {
			q.Stmt.Where.Expr = convertToExpr(where)
		} else {
			w := parser.AndExpr{
				Left:  q.Stmt.Where.Expr,
				Right: convertToExpr(where),
			}
			q.Stmt.Where.Expr = &w
		}
	}

	return q
}

func (q *DeleteQuery) SQL() (sql string, params []interface{}, err error) {
	if isValid, err := q.queryIsValid(); !isValid {
		return "", nil, err
	}

	if len(q.Errors) > 0 {
		err = errors.New(q.Errors.Error())
	}

	return parser.GenerateParsedQuery(q.Stmt).Query, nil, err
}

// Tries to verify if the query is ready for export to SQL
func (q *DeleteQuery) queryIsValid() (isValid bool, err error) {
	if q.Stmt.TableExprs == nil || len(q.Stmt.TableExprs) == 0 {
		return false, errors.New("No table selected")
	}

	return true, nil
}
