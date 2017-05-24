package qb

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	parser "github.com/morphar/sqlparsers/mysql"
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

func createRowWithValues(vals ...interface{}) (parser.ValTuple, error) {
	row := parser.ValTuple{}
	for _, val := range vals {
		sqlExpr := convertToExpr(val)
		if sqlVal, ok := sqlExpr.(*parser.SQLVal); ok {
			row = append(row, sqlVal)
		} else {
			return nil, errors.New(fmt.Sprintf("Unexpected type: %T after conversion: %T", val, sqlExpr))
		}
	}
	return row, nil
}

func newAliasedTableExpr(db, table string) *parser.AliasedTableExpr {
	return &parser.AliasedTableExpr{
		Expr: newTableName(db, table),
	}
}

func newAliasedExpr(db, table, col string) *parser.AliasedExpr {
	return &parser.AliasedExpr{
		Expr: newColName(db, table, col),
	}
}

func newStarExpr(db, table string) *parser.StarExpr {
	return &parser.StarExpr{
		TableName: newTableName(db, table),
	}
}

func newTableName(db, table string) parser.TableName {
	return parser.TableName{
		Name:      parser.NewTableIdent(table),
		Qualifier: parser.NewTableIdent(db),
	}
}

func newColName(db, table, col string) *parser.ColName {
	return &parser.ColName{
		Name: parser.NewColIdent(col),
		Qualifier: parser.TableName{
			Name:      parser.NewTableIdent(table),
			Qualifier: parser.NewTableIdent(db),
		},
	}
}

func newComparisonExpr(operator string, left, right interface{}) *parser.ComparisonExpr {
	var l, r parser.Expr
	l = convertToExpr(left)
	r = convertToExpr(right) // TODO:cIf this is a slice (not []byte), we need to foreach
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
		Name: c.Expr.(*parser.ColName),
		Expr: v,
	}
}

func convertToExpr(val interface{}) parser.Expr {
	switch v := val.(type) {
	case parser.Expr:
		return v

	case Column:
		return v.Expr

	case RangeCond:
		return v.RangeCond // Is this correct???

	case bool:
		return parser.BoolVal(v)

	case string:
		return parser.NewStrVal([]byte(v))
	case []byte:
		return parser.NewStrVal(v)

	// Ints
	case int:
		str := strconv.FormatInt(int64(v), 10)
		return parser.NewIntVal([]byte(str))
	case int8:
		str := strconv.FormatInt(int64(v), 10)
		return parser.NewIntVal([]byte(str))
	case int16:
		str := strconv.FormatInt(int64(v), 10)
		return parser.NewIntVal([]byte(str))
	case int32:
		str := strconv.FormatInt(int64(v), 10)
		return parser.NewIntVal([]byte(str))
	case int64:
		str := strconv.FormatInt(v, 10)
		return parser.NewIntVal([]byte(str))

	// UInts
	case uint:
		str := strconv.FormatUint(uint64(v), 10)
		return parser.NewIntVal([]byte(str))
	case uint8:
		str := strconv.FormatUint(uint64(v), 10)
		return parser.NewIntVal([]byte(str))
	case uint16:
		str := strconv.FormatUint(uint64(v), 10)
		return parser.NewIntVal([]byte(str))
	case uint32:
		str := strconv.FormatUint(uint64(v), 10)
		return parser.NewIntVal([]byte(str))
	case uint64:
		str := strconv.FormatUint(v, 10)
		return parser.NewIntVal([]byte(str))

	// Floats
	case float32:
		str := strconv.FormatFloat(float64(v), 'f', -1, 32)
		return parser.NewFloatVal([]byte(str))
	case float64:
		str := strconv.FormatFloat(v, 'f', -1, 64)
		return parser.NewFloatVal([]byte(str))
	}

	return nil
}
