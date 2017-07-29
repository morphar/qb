package postgres

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
	"github.com/morphar/tagstring"
	"github.com/morphar/text"
)

type InsertQuery struct {
	QueryBase
	stmt *parser.Insert
}

func (q *InsertQuery) Insert(rows ...interface{}) *InsertQuery {
	cols, vals, err := q.rowColsAndVals(rows...)

	if err != nil {
		q.Errors = append(q.Errors, err)
		log.Println(err)
		return q
	}

	for _, col := range cols {
		q.stmt.Columns = append(q.stmt.Columns, col.Expr.(parser.UnresolvedName))
	}

	parserRows := parser.ValuesClause{Tuples: []*parser.Tuple{}}
	for _, row := range vals {
		if rowVals, err := createRowWithValues(row...); err != nil {
			q.Errors = append(q.Errors, err)
		} else {
			parserRows.Tuples = append(parserRows.Tuples, &rowVals)
		}
	}

	if q.stmt.Rows == nil {
		q.stmt.Rows = &parser.Select{}
	}

	q.stmt.Rows.Select = &parserRows
	return q
}

func (q *InsertQuery) Into(into interface{}) *InsertQuery {
	q.stmt.Table = T(into)

	return q
}

// TODO: add some error handling
func (q *InsertQuery) SQL() (sql string, err error) {
	if isValid, err := q.queryIsValid(); !isValid {
		return "", err
	}

	if len(q.Errors) > 0 {
		err = errors.New(q.Errors.Error())
	}

	return q.stmt.String(), err
}

//
// Helper methods
//

func (q *InsertQuery) rowColsAndVals(rows ...interface{}) (cols []Column, vals [][]interface{}, err error) {
	switch len(rows) {
	case 0:
		return nil, nil, errors.New("No rows provided")
	case 1:
		// Minor "hack" to make it simpler to add slices of maps
		val := reflect.ValueOf(rows[0])
		if val.Kind() == reflect.Slice {
			vals := make([]interface{}, val.Len())
			for i := 0; i < val.Len(); i++ {
				vals[i] = val.Index(i).Interface()
			}
			return q.rowColsAndVals(vals...)
		}
	}

	cols, vals, err = q.getInsertColsAndVals(rows...)
	return
}

func (q *InsertQuery) getInsertColsAndVals(rows ...interface{}) (columns []Column, vals [][]interface{}, err error) {
	rowType := reflect.TypeOf(rows[0])

	if rowType.Kind() == reflect.Map {
		if rowType.String() != "map[string]interface {}" && rowType.String() != "map[Column]interface {}" {
			return nil, nil, errors.New(fmt.Sprintf("Wrong map type. Expected map[string]interface {} or map[Column]interface {} got %+v", rowType))
		}

	} else if rowType.Kind() != reflect.Struct {
		return nil, nil, errors.New(fmt.Sprintf("Wrong type of insert value. Supported types: sub select, map[string]interface{}, map[Column]interface{} and Struct. Got: %T", rows[0]))
	}

	for _, row := range rows {
		curRowType := reflect.TypeOf(row)

		if curRowType != rowType {
			return nil, nil, errors.New(fmt.Sprintf("Rows must all be of the same type. Expected %+v got %+v", rowType, curRowType))
		}

		if curRowType.Kind() == reflect.Map {
			curRowKeys := getSortedRowMapKeys(row)
			if len(columns) == 0 {
				columns = curRowKeys
			}

			if len(curRowKeys) != len(columns) {
				return nil, nil, errors.New(fmt.Sprintf("Rows with different value length. Expected %d got %d", len(columns), len(curRowKeys)))
			}

			if !rowMapKeysIsEqual(columns, curRowKeys) {
				return nil, nil, errors.New(fmt.Sprintf("Rows with different keys"))
			}

			rowVals := make([]interface{}, len(columns))
			rowMap := row.(map[string]interface{})
			for i, key := range columns {
				keyStr := strings.Trim(key.String(), `"`)
				rowVals[i] = rowMap[keyStr]
			}
			vals = append(vals, rowVals)

		} else if curRowType.Kind() == reflect.Struct {
			c, v := q.getFieldsValues(row)
			if len(columns) == 0 {
				columns = c
			}
			vals = append(vals, v)

		} else {
			log.Println(curRowType.Kind(), curRowType)
		}

	}
	return
}

func (q *InsertQuery) getFieldsValues(val interface{}) (cols []Column, vals []interface{}) {
	refVal := reflect.ValueOf(val)

	if refVal.IsValid() {
		for i := 0; i < refVal.NumField(); i++ {
			field := refVal.Field(i)
			typ := refVal.Type().Field(i)
			tag := tagstring.TagString(typ.Tag)

			colName := ""
			if tag.GetClean("col") != "" {
				colName = tag.GetClean("col")
			} else if tag.GetClean("db") != "" {
				colName = tag.GetClean("db")
			} else if tag.GetJsonName() != "" {
				colName = tag.GetJsonName()
			} else if typ.Name != "" {
				colName = text.SnakeCase(typ.Name)
			}

			var val interface{}
			if field.Type().Kind() == reflect.Ptr {
				if field.IsNil() {
					continue
				}
				val = field.Elem().Interface()
			} else {
				val = field.Interface()
			}

			cols = append(cols, C(colName))
			vals = append(vals, val)
		}
	}

	return
}

// TODO: make this do something
// Tries to verify if the query is ready for export to SQL
func (q *InsertQuery) queryIsValid() (isValid bool, err error) {
	// if q.stmt.Action == "" {
	//     return false, errors.New("Action not specified - e.g. insert")
	// }

	// if q.stmt.Table.Name.String() == "" {
	//     return false, errors.New("No table name provided")
	// }

	// if q.stmt.Rows == nil {
	//     return false, errors.New("No values provided")
	// }

	// colsCount := len(q.stmt.Columns)
	// rowsCount := len(q.stmt.Rows.(parser.Values))

	// if colsCount == 0 {
	//     return false, errors.New("No columns provided")
	// }
	// if rowsCount == 0 {
	//     return false, errors.New("No row values provided")
	// }

	// for _, rowValues := range q.stmt.Rows.(parser.Values) {
	//     if len(rowValues) != colsCount {
	//         errStr := "Column and row values count does not match. Columns: %d, Row values: %d"
	//         return false, errors.New(fmt.Sprintf(errStr, colsCount, len(rowValues)))
	//     }
	// }

	return true, nil
}
