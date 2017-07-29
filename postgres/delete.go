package postgres

import (
	"errors"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

type DeleteQuery struct {
	QueryBase
	stmt *parser.Delete
}

/*
&parser.Delete{
    Table: &parser.NormalizableTableName{
        TableNameReference: parser.UnresolvedName{
            parser.Name("yoyo"),
        },
    },
*/

func (q *DeleteQuery) From(from interface{}) *DeleteQuery {
	q.stmt.Table = T(from)

	return q
}

func (q *DeleteQuery) Where(wheres ...parser.Expr) *DeleteQuery {
	if len(wheres) == 0 {
		return q
	}

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
func (q *DeleteQuery) SQL() (sql string, err error) {
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
func (q *DeleteQuery) queryIsValid() (isValid bool, err error) {
	// if q.stmt.TableExprs == nil || len(q.stmt.TableExprs) == 0 {
	//     return false, errors.New("No table selected")
	// }

	return true, nil
}
