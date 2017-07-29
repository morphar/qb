package qb

import (
	"errors"

	parser "github.com/morphar/sqlparsers/pkg/mysql"
)

type UpdateQuery struct {
	QueryBase

	Stmt *parser.Update
}

// Convenience
func (q *UpdateQuery) Table(tables ...interface{}) *UpdateQuery {
	return q.Update(tables...)
}

func (q *UpdateQuery) Update(tables ...interface{}) *UpdateQuery {
	q.Stmt.TableExprs = parser.TableExprs{}

	for _, from := range tables {
		q.Stmt.TableExprs = append(q.Stmt.TableExprs, T(from))
	}

	return q
}

func (q *UpdateQuery) Set(sets ...interface{}) *UpdateQuery {
	for _, set := range sets {
		if s, ok := set.(parser.UpdateExpr); ok {
			q.Stmt.Exprs = append(q.Stmt.Exprs, &s)

		} else if s, ok := set.(ComparisonExpr); ok {
			if colName, ok := s.Left.(*parser.ColName); ok {
				newSet := parser.UpdateExpr{
					Name: colName,
					Expr: s.Right,
				}
				q.Stmt.Exprs = append(q.Stmt.Exprs, &newSet)
			}

		}
	}

	return q
}

func (q *UpdateQuery) Where(wheres ...parser.Expr) *UpdateQuery {
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

func (q *UpdateQuery) SQL() (sql string, err error) {
	if isValid, err := q.queryIsValid(); !isValid {
		return "", err
	}

	if len(q.Errors) > 0 {
		err = errors.New(q.Errors.Error())
	}

	return parser.GenerateParsedQuery(q.Stmt).Query, err
}

// Tries to verify if the query is ready for export to SQL
func (q *UpdateQuery) queryIsValid() (isValid bool, err error) {
	if q.Stmt.TableExprs == nil || len(q.Stmt.TableExprs) == 0 {
		return false, errors.New("No table selected")
	}

	if len(q.Stmt.Exprs) == 0 {
		return false, errors.New("Nothing to update")
	}

	return true, nil
}
