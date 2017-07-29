package postgres

import (
	"errors"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

type UpdateQuery struct {
	QueryBase
	stmt *parser.Update
}

// Convenience
func (q *UpdateQuery) Table(table interface{}) *UpdateQuery {
	return q.Update(table)
}

func (q *UpdateQuery) Update(table interface{}) *UpdateQuery {
	if table != nil {
		t := T(table)
		q.stmt.Table = t.TableExpr.(*parser.AliasedTableExpr).Expr
	}
	return q
}

func (q *UpdateQuery) Set(sets ...interface{}) *UpdateQuery {
	for _, set := range sets {
		if s, ok := set.(parser.UpdateExpr); ok {
			q.stmt.Exprs = append(q.stmt.Exprs, &s)

		} else if s, ok := set.(ComparisonExpr); ok {
			if colName, ok := s.Left.(parser.UnresolvedName); ok {
				newSet := parser.UpdateExpr{
					Names: parser.UnresolvedNames{colName},
					Expr:  s.Right,
				}
				q.stmt.Exprs = append(q.stmt.Exprs, &newSet)
			}

		}
	}

	return q
}

func (q *UpdateQuery) Where(wheres ...parser.Expr) *UpdateQuery {
	if q.stmt.Where == nil {
		q.stmt.Where = &parser.Where{Type: "WHERE"}
	}

	for _, where := range wheres {
		if q.stmt.Where.Expr == nil {
			q.stmt.Where.Expr = convertToExpr(where)
		} else {
			w := parser.AndExpr{
				Left:  q.stmt.Where.Expr,
				Right: convertToExpr(where),
			}
			q.stmt.Where.Expr = &w
		}
	}

	return q
}

// TODO: add some error handling
func (q *UpdateQuery) SQL() (sql string, err error) {
	if isValid, err := q.queryIsValid(); !isValid {
		return "", err
	}

	if len(q.Errors) > 0 {
		err = errors.New(q.Errors.Error())
	}

	return q.stmt.String(), err
}

// TODO: make this do something
// Tries to verify if the query is ready for export to SQL
func (q *UpdateQuery) queryIsValid() (isValid bool, err error) {
	// if q.stmt.TableExprs == nil || len(q.stmt.TableExprs) == 0 {
	//     return false, errors.New("No table selected")
	// }

	// if len(q.stmt.Exprs) == 0 {
	//     return false, errors.New("Nothing to update")
	// }

	return true, nil
}
