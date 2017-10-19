package postgres

import (
	"errors"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

type SelectQuery struct {
	QueryBase
	stmt *parser.Select
}

func (q *SelectQuery) Select(fields ...interface{}) *SelectQuery {
	if q.stmt.Select == nil {
		q.stmt.Select = &parser.SelectClause{}
	}

	for _, field := range fields {
		selectClause := q.stmt.Select.(*parser.SelectClause)
		selectClause.Exprs = append(selectClause.Exprs, parser.SelectExpr{
			Expr: C(field).Expr,
		})
	}

	return q
}

func (q *SelectQuery) From(froms ...interface{}) *SelectQuery {
	if q.stmt.Select == nil {
		q.stmt.Select = &parser.SelectClause{}
	}

	selectClause := q.stmt.Select.(*parser.SelectClause)

	// if selectClause.From == nil {
	selectClause.From = &parser.From{}
	// }
	for _, from := range froms {
		selectClause.From.Tables = append(selectClause.From.Tables, T(from))
	}

	return q
}

func (q *SelectQuery) Where(wheres ...parser.Expr) *SelectQuery {
	if len(wheres) == 0 {
		return q
	}

	selectClause := q.stmt.Select.(*parser.SelectClause)

	if selectClause.Where == nil {
		selectClause.Where = &parser.Where{Type: "WHERE"}
	}

	for _, where := range wheres {
		if selectClause.Where.Expr == nil {
			selectClause.Where.Expr = convertToExpr(where)
		} else {
			w := parser.AndExpr{
				Left:  selectClause.Where.Expr,
				Right: convertToExpr(where),
			}
			selectClause.Where.Expr = &w
		}
	}

	return q
}

func (q *SelectQuery) Join(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) FullJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("FULL JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) LeftJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("LEFT JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) RightJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("RIGHT JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) CrossJoin(table interface{}) *SelectQuery {
	return q.join("CROSS JOIN", table, nil)
}
func (q *SelectQuery) NaturalJoin(table interface{}) *SelectQuery {
	return q.join("JOIN", table, parser.NaturalJoinCond{})
}
func (q *SelectQuery) NaturalLeftJoin(table interface{}) *SelectQuery {
	return q.join("LEFT JOIN", table, parser.NaturalJoinCond{})
}
func (q *SelectQuery) NaturalRightJoin(table interface{}) *SelectQuery {
	return q.join("RIGHT JOIN", table, parser.NaturalJoinCond{})
}
func (q *SelectQuery) NaturalFullJoin(table interface{}) *SelectQuery {
	return q.join("FULL JOIN", table, parser.NaturalJoinCond{})
}
func (q *SelectQuery) OuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("OUTER JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) FullOuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("FULL OUTER JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) LeftOuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("LEFT OUTER JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) RightOuterJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("RIGHT OUTER JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) InnerJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("INNER JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) LeftInnerJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("LEFT INNER JOIN", table, &parser.OnJoinCond{Expr: on})
}
func (q *SelectQuery) RightInnerJoin(table interface{}, on parser.Expr) *SelectQuery {
	return q.join("RIGHT INNER JOIN", table, &parser.OnJoinCond{Expr: on})
}

// on is basically a where expr...
func (q *SelectQuery) join(typ string, table interface{}, cond parser.JoinCond) *SelectQuery {
	selectClause := q.stmt.Select.(*parser.SelectClause)

	// If FROM is not set, we have nothing to join with...
	if selectClause.From == nil {
		return q
	}

	newLeft := selectClause.From.Tables[0]

	selectClause.From.Tables = parser.TableExprs{
		&parser.JoinTableExpr{
			Join:  typ,
			Left:  newLeft, // Maybe we can do this directly??
			Right: T(table),
			Cond:  cond,
		},
	}

	return q
}

func (q *SelectQuery) Order(orders ...*parser.Order) *SelectQuery {
	if q.stmt.OrderBy == nil {
		q.stmt.OrderBy = parser.OrderBy{}
	}

	for _, order := range orders {
		q.stmt.OrderBy = append(q.stmt.OrderBy, order)
	}

	return q
}

func (q *SelectQuery) GroupBy(groupBys ...interface{}) *SelectQuery {
	selectClause := q.stmt.Select.(*parser.SelectClause)

	if selectClause.GroupBy == nil {
		selectClause.GroupBy = parser.GroupBy{}
	}

	for _, groupBy := range groupBys {
		selectClause.GroupBy = append(selectClause.GroupBy, convertToExpr(groupBy))
	}

	return q
}

func (q *SelectQuery) Limit(offset, limit int) *SelectQuery {
	if offset > 0 || limit > 0 {
		q.stmt.Limit = &parser.Limit{
			Offset: convertToExpr(offset),
			Count:  convertToExpr(limit),
		}
	} else {
		q.stmt.Limit = nil
	}

	return q
}

// TODO: add some error handling
func (q *SelectQuery) SQL() (sql string, err error) {
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
func (q *SelectQuery) queryIsValid() (isValid bool, err error) {
	// if q.Stmt.SelectExprs == nil || len(q.Stmt.SelectExprs) == 0 {
	// 	return false, errors.New("No columns selected")
	// }

	// if q.Stmt.From == nil || len(q.Stmt.From) == 0 {
	// 	return false, errors.New("No table selected")
	// }

	return true, nil
}

// - SELECT
// - FROM
// - WHERE
// - JOINs
// - GROUP BY
// - ORDER BY
// - LIMIT
// - DISTINCT
// - COUNT
// ? HAVING
// ? RETURNING // Only postgres, right?
// ? PREPARED
// ? AS // It could be an issue later

// REMEMBER PARSER!

/*
	From: parser.TableExprs{
		&parser.JoinTableExpr{
			LeftExpr: &parser.AliasedTableExpr{
				Expr: parser.TableName{
					Name:      parser.NewTableIdent("users"),
					Qualifier: parser.NewTableIdent(""),
				},
				As:    parser.NewTableIdent(""),
				Hints: nil,
			},
			Join: "join",
			RightExpr: &parser.AliasedTableExpr{
				Expr: parser.TableName{
					Name:      parser.NewTableIdent("comments"),
					Qualifier: parser.NewTableIdent(""),
				},
				As:    parser.NewTableIdent(""),
				Hints: nil,
			},
			On: &parser.ComparisonExpr{
				Operator: "=",
				Left: &parser.ColName{
					Metadata: nil,
					Name:     parser.NewColIdent("users_id"),
					Qualifier: parser.TableName{
						Name:      parser.NewTableIdent("comments"),
						Qualifier: parser.NewTableIdent(""),
					},
				},
				Right: &parser.ColName{
					Metadata: nil,
					Name:     parser.NewColIdent("id"),
					Qualifier: parser.TableName{
						Name:      parser.NewTableIdent("users"),
						Qualifier: parser.NewTableIdent(""),
					},
				},
				Escape: nil,
			},
		},
	},

------------------------------------------------------------------------------------------

	From: parser.TableExprs{
		&parser.JoinTableExpr{
			LeftExpr: &parser.JoinTableExpr{
				LeftExpr: &parser.AliasedTableExpr{
					Expr: parser.TableName{
						Name:      parser.NewTableIdent("users"),
						Qualifier: parser.NewTableIdent(""),
					},
					As:    parser.NewTableIdent(""),
					Hints: nil,
				},
				Join: "join",
				RightExpr: &parser.AliasedTableExpr{
					Expr: parser.TableName{
						Name:      parser.NewTableIdent("comments"),
						Qualifier: parser.NewTableIdent(""),
					},
					As:    parser.NewTableIdent(""),
					Hints: nil,
				},
				On: &parser.ComparisonExpr{
					Operator: "=",
					Left: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("users_id"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("comments"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
					Right: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("id"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("users"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
					Escape: nil,
				},
			},
			Join: "left join",
			RightExpr: &parser.AliasedTableExpr{
				Expr: parser.TableName{
					Name:      parser.NewTableIdent("posts"),
					Qualifier: parser.NewTableIdent(""),
				},
				As:    parser.NewTableIdent(""),
				Hints: nil,
			},
			On: &parser.ComparisonExpr{
				Operator: "=",
				Left: &parser.ColName{
					Metadata: nil,
					Name:     parser.NewColIdent("users_id"),
					Qualifier: parser.TableName{
						Name:      parser.NewTableIdent("posts"),
						Qualifier: parser.NewTableIdent(""),
					},
				},
				Right: &parser.ColName{
					Metadata: nil,
					Name:     parser.NewColIdent("id"),
					Qualifier: parser.TableName{
						Name:      parser.NewTableIdent("users"),
						Qualifier: parser.NewTableIdent(""),
					},
				},
				Escape: nil,
			},
		},
	},

--------------------------------------------------------------------------------------------

	From: parser.TableExprs{
		&parser.JoinTableExpr{
			LeftExpr: &parser.JoinTableExpr{
				LeftExpr: &parser.AliasedTableExpr{
					Expr: parser.TableName{
						Name:      parser.NewTableIdent("users"),
						Qualifier: parser.NewTableIdent(""),
					},
					As:    parser.NewTableIdent(""),
					Hints: nil,
				},
				Join: "join",
				RightExpr: &parser.AliasedTableExpr{
					Expr: parser.TableName{
						Name:      parser.NewTableIdent("comments"),
						Qualifier: parser.NewTableIdent(""),
					},
					As:    parser.NewTableIdent(""),
					Hints: nil,
				},
				On: &parser.ComparisonExpr{
					Operator: "=",
					Left: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("users_id"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("comments"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
					Right: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("id"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("users"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
					Escape: nil,
				},
			},
			Join: "left join",
			RightExpr: &parser.AliasedTableExpr{
				Expr: parser.TableName{
					Name:      parser.NewTableIdent("posts"),
					Qualifier: parser.NewTableIdent(""),
				},
				As:    parser.NewTableIdent(""),
				Hints: nil,
			},
			On: &parser.AndExpr{
				Left: &parser.ComparisonExpr{
					Operator: "=",
					Left: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("users_id"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("posts"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
					Right: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("id"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("users"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
					Escape: nil,
				},
				Right: &parser.IsExpr{
					Operator: "is not null",
					Expr: &parser.ColName{
						Metadata: nil,
						Name:     parser.NewColIdent("published"),
						Qualifier: parser.TableName{
							Name:      parser.NewTableIdent("posts"),
							Qualifier: parser.NewTableIdent(""),
						},
					},
				},
			},
		},
	},

*/
