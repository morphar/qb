package qb

import (
	"sort"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

func getSortedRowMapKeys(rowMap interface{}) (keys []Column) {
	if m, ok := rowMap.(map[Column]interface{}); ok {
		for keyCol, _ := range m {
			keys = append(keys, keyCol)
		}
	} else if m, ok := rowMap.(map[string]interface{}); ok {
		for key, _ := range m {
			keys = append(keys, C(key))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return
}

func rowMapKeysIsEqual(keys1 []Column, keys2 []Column) bool {
	for i, key := range keys1 {
		if keys2[i].String() != key.String() {
			return false
		}
	}
	return true
}

func createRowWithValues(vals ...interface{}) (parser.Tuple, error) {
	row := parser.Tuple{}
	for _, val := range vals {
		sqlExpr := convertToExpr(val)
		row.Exprs = append(row.Exprs, sqlExpr)
	}
	return row, nil
}

func newAliasedTableExpr(db, table string) *parser.AliasedTableExpr {
	return &parser.AliasedTableExpr{
		Expr: &parser.NormalizableTableName{
			TableNameReference: newUnresolvedName(db, table),
		},
	}
}

func newSelectExpr(db, table, col string) *parser.SelectExpr {
	return &parser.SelectExpr{
		Expr: newUnresolvedName(db, table, col),
	}
}

// func NormalizableTableName(db, table, col string) *parser.NormalizableTableName {
// 	return &parser.AliasedExpr{
// 		Expr: newColName(db, table, col),
// 	}
// }

func newStarExpr(db, table string) *parser.SelectExpr {
	return &parser.SelectExpr{
		Expr: parser.UnqualifiedStar{},
	}
}

// func newTableName(db, table string) parser.TableName {
// 	return parser.TableName{
// 		Name:      parser.NewTableIdent(table),
// 		Qualifier: parser.NewTableIdent(db),
// 	}
// }

// Tables: parser.TableExprs{
// 	&parser.AliasedTableExpr{
// 		Expr: &parser.NormalizableTableName{
// 			TableNameReference: parser.UnresolvedName{
// 				parser.Name("yoyo"),
// 			},
// 		},
// 		Hints:      nil,
// 		Ordinality: false,
// 		As: parser.AliasClause{
// 			Alias: parser.Name(""),
// 			Cols:  nil,
// 		},
// 	},
// },

func newUnresolvedName(names ...string) *parser.UnresolvedName {
	un := parser.UnresolvedName{}
	for _, name := range names {
		if name != "" {
			un = append(un, parser.Name(name))
		}
	}
	return &un
}

// func newColName(db, table, col string) *parser.ColName {
// 	return &parser.ColName{
// 		Name: parser.NewColIdent(col),
// 		Qualifier: parser.TableName{
// 			Name:      parser.NewTableIdent(table),
// 			Qualifier: parser.NewTableIdent(db),
// 		},
// 	}
// }

func newComparisonExpr(operator parser.ComparisonOperator, left, right interface{}) *parser.ComparisonExpr {
	var l, r parser.Expr
	l = convertToExpr(left)
	r = convertToExpr(right)
	return &parser.ComparisonExpr{
		Operator: operator,
		Left:     l,
		Right:    r,
	}
}

func newUpdateExpr(c Column, val interface{}) parser.UpdateExpr {
	// name := convertToExpr(c)
	v := convertToExpr(val)
	return parser.UpdateExpr{
		Names: parser.UnresolvedNames{c.SelectExpr.Expr.(parser.UnresolvedName)},
		Expr:  v,
	}
}

// func convertNameToOperator(name string) (co parser.ComparisonOperator) {
// 	co, _ = comparisonOpsByName[name]
// 	return
// }

func convertToExpr(val interface{}) parser.Expr {
	switch v := val.(type) {
	case parser.Expr:
		return v

	case Column:
		return v.Expr

	case ComparisonExpr:
		return v.ComparisonExpr

	case RangeCond:
		return v.RangeCond // Is this correct???

	case bool:
		if v {
			return parser.DBoolTrue
		} else {
			return parser.DBoolFalse
		}

	case string:
		return parser.NewDString(v)
	case []byte:
		return parser.NewDBytes(parser.DBytes(v))

	// Ints
	case int:
		return parser.NewDInt(parser.DInt(val.(int)))

	case int8:
		return parser.NewDInt(parser.DInt(val.(int8)))

	case int16:
		return parser.NewDInt(parser.DInt(val.(int16)))

	case int32:
		return parser.NewDInt(parser.DInt(val.(int32)))

	case int64:
		return parser.NewDInt(parser.DInt(val.(int64)))

	case uint:
		return parser.NewDInt(parser.DInt(val.(uint)))

	case uint8:
		return parser.NewDInt(parser.DInt(val.(uint8)))

	case uint16:
		return parser.NewDInt(parser.DInt(val.(uint16)))

	case uint32:
		return parser.NewDInt(parser.DInt(val.(uint32)))

	case uint64:
		return parser.NewDInt(parser.DInt(val.(uint64)))

	// Floats
	case float32:
		return parser.NewDFloat(parser.DFloat(val.(float32)))
	case float64:
		return parser.NewDFloat(parser.DFloat(val.(float64)))
	}

	return nil
}

/*
func FormatBool(b bool) string
func FormatFloat(f float64, fmt byte, prec, bitSize int) string
func FormatInt(i int64, base int) string
func FormatUint(i uint64, base int) string
*/

// func NewStrVal(in []byte) *SQLVal {
// 	return &SQLVal{Type: StrVal, Val: in}
// }

// func NewIntVal(in []byte) *SQLVal {
// 	return &SQLVal{Type: IntVal, Val: in}
// }

// func NewFloatVal(in []byte) *SQLVal {
// 	return &SQLVal{Type: FloatVal, Val: in}
// }

// func NewHexNum(in []byte) *SQLVal {
// 	return &SQLVal{Type: HexNum, Val: in}
// }

// func NewHexVal(in []byte) *SQLVal {
// 	return &SQLVal{Type: HexVal, Val: in}
// }

// func NewValArg(in []byte) *SQLVal {
// 	return &SQLVal{Type: ValArg, Val: in}
// }

// func joinTable(from parser.TableExprs, joinTable parser.TableExpr, on *parser.ComparisonExpr) {
// 	last := len(from) - 1
// 	if last >= 0 {
// 		from[last] = &parser.JoinTableExpr{
// 			LeftExpr:  from[last],
// 			RightExpr: joinTable,
// 			On:        on,
// 		}
// 	}
// }

// func getRightMost(from parser.TableExprs) *parser.AliasedTableExpr {
// 	if len(from) > 0 {
// 		last := from[len(from)-1]
// 		switch last.(type) {
// 		case *parser.AliasedTableExpr:
// 		case *parser.JoinTableExpr:
// 		}
// 	}
// 	return &parser.AliasedTableExpr{}
// }

// func newJoinTableExpr() {
// 	// Find the last JOIN or the last FROM
// 	if len()
// }

/*
select *, somedb.posts.*, users.*, users.id, title
from somedb.users, somedb.posts
left join comments on comments.users_id = users.id
left join somsom on somsom.users_id = users.id

From: parser.TableExprs{
	&parser.AliasedTableExpr{
		Expr: parser.TableName{
			Name:      parser.NewTableIdent("users"),
			Qualifier: parser.NewTableIdent("somedb"),
		},
		As:    parser.NewTableIdent(""),
		Hints: nil,
	},

	&parser.JoinTableExpr{
		LeftExpr: &parser.JoinTableExpr{
			LeftExpr: &parser.AliasedTableExpr{
				Expr: parser.TableName{
					Name:      parser.NewTableIdent("posts"),
					Qualifier: parser.NewTableIdent("somedb"),
				},
				As:    parser.NewTableIdent(""),
				Hints: nil,
			},
			Join: "left join",
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
				Name:      parser.NewTableIdent("somsom"),
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
					Name:      parser.NewTableIdent("somsom"),
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



select *, somedb.posts.*, users.*, users.id, title
from somedb.users, somedb.posts
left join comments on comments.users_id = users.id

From: parser.TableExprs{
	&parser.AliasedTableExpr{
		Expr: parser.TableName{
			Name:      parser.NewTableIdent("users"),
			Qualifier: parser.NewTableIdent("somedb"),
		},
		As:    parser.NewTableIdent(""),
		Hints: nil,
	},

	&parser.JoinTableExpr{
		LeftExpr: &parser.AliasedTableExpr{
			Expr: parser.TableName{
				Name:      parser.NewTableIdent("posts"),
				Qualifier: parser.NewTableIdent("somedb"),
			},
			As:    parser.NewTableIdent(""),
			Hints: nil,
		},
		Join: "left join",
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
*/
