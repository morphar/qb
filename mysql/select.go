package mysql

import (
	"errors"

	parser "github.com/morphar/sqlparsers/pkg/mysql"
)

type SelectQuery struct {
	QueryBase

	Stmt *parser.Select
}

func (q *SelectQuery) Select(fields ...interface{}) *SelectQuery {
	if q.Stmt.SelectExprs == nil {
		q.Stmt.SelectExprs = parser.SelectExprs{}
	}

	if len(fields) > 0 {
		for _, field := range fields {

			if col, ok := field.(Column); ok {
				q.Stmt.SelectExprs = append(q.Stmt.SelectExprs, col)
			} else {
				q.Stmt.SelectExprs = append(q.Stmt.SelectExprs, C(field))
			}
		}
	} else {
		q.Stmt.SelectExprs = append(q.Stmt.SelectExprs, C("*"))
	}

	return q
}

func (q *SelectQuery) From(froms ...interface{}) *SelectQuery {
	// if q.Stmt.From == nil {
	q.Stmt.From = parser.TableExprs{}
	// }

	for _, from := range froms {
		q.Stmt.From = append(q.Stmt.From, T(from))
	}

	return q
}

func (q *SelectQuery) Where(wheres ...parser.Expr) *SelectQuery {
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

func (q *SelectQuery) Join(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("join", table, on)
}
func (q *SelectQuery) FullJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("full join", table, on)
}
func (q *SelectQuery) LeftJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("left join", table, on)
}
func (q *SelectQuery) RightJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("right join", table, on)
}
func (q *SelectQuery) CrossJoin(table interface{}) *SelectQuery {
	return q.join("cross join", table, nil)
}
func (q *SelectQuery) NaturalJoin(table interface{}) *SelectQuery {
	return q.join("natural join", table, nil)
}
func (q *SelectQuery) NaturalLeftJoin(table interface{}) *SelectQuery {
	return q.join("natural left join", table, nil)
}
func (q *SelectQuery) NaturalRightJoin(table interface{}) *SelectQuery {
	return q.join("natural right join", table, nil)
}
func (q *SelectQuery) NaturalFullJoin(table interface{}) *SelectQuery {
	return q.join("natural full join", table, nil)
}
func (q *SelectQuery) OuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("outer join", table, on)
}
func (q *SelectQuery) FullOuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("full outer join", table, on)
}
func (q *SelectQuery) LeftOuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("left outer join", table, on)
}
func (q *SelectQuery) RightOuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("right outer join", table, on)
}
func (q *SelectQuery) InnerJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("inner join", table, on)
}
func (q *SelectQuery) LeftInnerJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("left inner join", table, on)
}
func (q *SelectQuery) RightInnerJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("right inner join", table, on)
}

// on is basically a where expr...
func (q *SelectQuery) join(typ string, table interface{}, on parser.Expr) *SelectQuery {
	// If FROM is not set, we have nothing to join with...
	if q.Stmt.From == nil {
		return q
	}

	newLeft := q.Stmt.From[0]

	q.Stmt.From = parser.TableExprs{
		&parser.JoinTableExpr{
			Join:      typ,
			LeftExpr:  newLeft, // Maybe we can do this directly??
			RightExpr: T(table),
			On:        on,
		},
	}

	return q
}

func (q *SelectQuery) Order(orders ...*parser.Order) *SelectQuery {
	if q.Stmt.OrderBy == nil {
		q.Stmt.OrderBy = parser.OrderBy{}
	}

	for _, order := range orders {
		q.Stmt.OrderBy = append(q.Stmt.OrderBy, order)
	}

	return q
}

func (q *SelectQuery) GroupBy(groupBys ...interface{}) *SelectQuery {
	if q.Stmt.GroupBy == nil {
		q.Stmt.GroupBy = parser.GroupBy{}
	}

	for _, groupBy := range groupBys {
		q.Stmt.GroupBy = append(q.Stmt.GroupBy, C(groupBy).SelectExpr.(*parser.AliasedExpr).Expr)
	}

	return q
}

func (q *SelectQuery) Limit(offset, limit int) *SelectQuery {
	if offset > 0 || limit > 0 {
		q.Stmt.Limit = &parser.Limit{
			Offset:   convertToExpr(offset),
			Rowcount: convertToExpr(limit),
		}
	} else {
		q.Stmt.Limit = nil
	}

	return q
}

func (q *SelectQuery) SQL() (sql string, err error) {
	if isValid, err := q.queryIsValid(); !isValid {
		return "", err
	}

	if len(q.Errors) > 0 {
		err = errors.New(q.Errors.Error())
	}

	return parser.GenerateParsedQuery(q.Stmt).Query, err
}

// Tries to verify if the query is ready for export to SQL
func (q *SelectQuery) queryIsValid() (isValid bool, err error) {
	if q.Stmt.SelectExprs == nil || len(q.Stmt.SelectExprs) == 0 {
		return false, errors.New("No columns selected")
	}

	if q.Stmt.From == nil || len(q.Stmt.From) == 0 {
		return false, errors.New("No table selected")
	}

	return true, nil
}
