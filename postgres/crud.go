package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	parser "github.com/morphar/sqlparsers/pkg/postgres"
)

// TODO: We need to ensure, we don't get caught by a panic here...
// It could also help to prevent errors, if we validate the query before running.
// That is currently only possibly from the *Query methods, as the validator method
// sits on that

// TODO: Try to get the postgres / mysql / sqlite as consistent as possible
// E.g. make sure that we have a unified way of getting last insert id.
// In postgres we could have our own result type for exec and always append a RETUNRIN

func (q *QueryBase) Exec() (sql.Result, error) {
	// Ensure we have an INSERT statement
	// TODO: Guard against panics!
	// TODO: Check if it's possible to "check syntax" by doing an exec with SELECT, then check for errors
	if _, ok := q.stmt.(*parser.Select); ok {
		return nil, errors.New("qb.Exec only works with anything but SELECT queries")
	}

	query := q.stmt.String()
	return q.db.Exec(query)
}

func (q *QueryBase) ScanStruct(strct interface{}) error {
	val := reflect.ValueOf(strct)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return errors.New("The struct parameter must be a pointer to a non-nil struct value when calling ScanStructs")
	}

	// res := reflect.Indirect(reflect.ValueOf(strct))
	structs := reflect.New(reflect.SliceOf(reflect.TypeOf(strct))).Interface()
	if err := q.ScanStructs(structs); err != nil {
		return err
	}

	resSlice := reflect.ValueOf(structs).Elem()
	if !resSlice.IsNil() {
		res := resSlice.Index(0)
		val.Elem().Set(res.Elem())
	}

	return nil
}

func (q *QueryBase) ScanStructs(structs interface{}) error {
	if q.db == nil {
		return errors.New("No database provided. Did you initialize a QueryBuilder with qb.New(db)?")
	}

	if len(q.Errors) > 0 {
		return errors.New(q.Errors.Error())
	}

	// Ensure we have a SELECT statement
	// TODO: Guard against panics!
	if _, ok := q.stmt.(*parser.Select); !ok {
		return errors.New("qb.ScanStructs only works with SELECT queries")
	}

	val := reflect.ValueOf(structs)
	if val.Kind() != reflect.Ptr {
		return errors.New("The structs parameter must be a pointer to a slice when calling ScanStructs")
	}

	res := reflect.Indirect(val)
	if res.Kind() != reflect.Slice {
		return errors.New("The structs parameter must be a pointer to a slice when calling ScanStructs")
	}

	// Build up that fields map

	query := q.stmt.String()
	rows, err := q.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	mainStructInfo := getStructInfo(structs)

	for rows.Next() {
		scanMap, err := q.createScanMap(columns, mainStructInfo)

		if err != nil {
			log.Println(err)
		}

		if err := rows.Scan(scanMap...); err != nil {
			return err
		}

		newStruct, err := q.scanMapToStruct(scanMap, columns, mainStructInfo)
		if err != nil {
			return err
		}

		if res.Type().Elem().Kind() == reflect.Ptr {
			res.Set(reflect.Append(res, newStruct.Elem().Addr()))
		} else {
			res.Set(reflect.Append(res, newStruct.Elem()))
		}
	}

	return err
}

func (q *QueryBase) createScanMap(resCols []string, mainStructInfo StructInfo) (scanMap []interface{}, err error) {
	// Extract info about the main table
	mainTable, err := q.getMainTable()
	if err != nil {
		return nil, err
	}
	mainTableStr := mainTable.String()

	// Build a StructInfo map as needed
	structInfoMap := map[string]StructInfo{}
	structInfoMap[mainTableStr] = mainStructInfo

	for _, field := range mainStructInfo.Fields {
		if field.IsParent {
			if _, ok := structInfoMap[field.DBName]; !ok {
				newStructInterface := reflect.New(field.BaseType).Interface()
				structInfoMap[field.DBName] = getStructInfo(newStructInterface)
			}
		}
	}
	slct := q.stmt.(*parser.Select)

	for _, selectExpr := range slct.Select.(*parser.SelectClause).Exprs {
		colInfo := Column{selectExpr}

		// If table name isn't set, assume that it's the main table
		tableName := colInfo.GetTable()
		if tableName == "" {
			tableName = mainTableStr
		}

		// Get struct info for the current table
		var ok bool
		var curStructInfo StructInfo
		if curStructInfo, ok = structInfoMap[tableName]; !ok {
			return nil, errors.New("Probably an error here!... 3")
		}

		var curCols []string

		if colInfo.GetColumn() == "*" {
			curCols = resCols[0:curStructInfo.NonRelFields]
			resCols = resCols[curStructInfo.NonRelFields:]
		} else {
			curCols = resCols[0:1]
			resCols = resCols[1:]
		}

		for _, curCol := range curCols {
			if curField, ok := curStructInfo.Fields[curCol]; ok {
				// TODO: Ugly hack - can it be made prettier?
				// Either it doesn't belong here or a more generic way of
				//   doing this is needed
				if curField.BaseType.String() == "timestamp.Timestamp" {
					var newTime *time.Time
					scanMap = append(scanMap, &newTime)
				} else {
					scanMap = append(scanMap, reflect.New(reflect.New(curField.Type).Type()).Interface())
				}
			} else {
				errMsg := fmt.Sprintf("A match wasn't for for the request column: %s.%s", tableName, curCol)
				return nil, errors.New(errMsg)
			}
		}

	}

	return
}

func (q *QueryBase) scanMapToStruct(resVals []interface{}, resCols []string, mainStructInfo StructInfo) (resultStruct *reflect.Value, err error) {
	// Extract info about the main table
	mainTable, err := q.getMainTable()
	if err != nil {
		return nil, err
	}
	mainTableStr := mainTable.String()

	// Build a StructInfo map as needed
	structInfoMap := map[string]StructInfo{}
	structInfoMap[mainTableStr] = mainStructInfo

	for _, field := range mainStructInfo.Fields {
		if field.IsParent {
			if _, ok := structInfoMap[field.DBName]; !ok {
				newStructInterface := reflect.New(field.BaseType).Interface()
				structInfoMap[field.DBName] = getStructInfo(newStructInterface)
			}
		}
	}
	slct := q.stmt.(*parser.Select).Select.(*parser.SelectClause)

	// Initialize the new result
	newStruct := reflect.New(mainStructInfo.Type)

	for _, selectExpr := range slct.Exprs {
		colInfo := Column{selectExpr}

		// If table name isn't set, assume that it's the main table
		tableName := colInfo.GetTable()
		if tableName == "" {
			tableName = mainTableStr
		}

		// Get struct info for the current table
		var ok bool
		var curStructInfo StructInfo
		if curStructInfo, ok = structInfoMap[tableName]; !ok {
			return nil, errors.New("Probably an error here!... 3")
		}

		var curCols []string
		var curVals []interface{}

		if colInfo.GetColumn() == "*" {
			curCols = resCols[0:curStructInfo.NonRelFields]
			resCols = resCols[curStructInfo.NonRelFields:]
			curVals = resVals[0:curStructInfo.NonRelFields]
			resVals = resVals[curStructInfo.NonRelFields:]
		} else {
			curCols = resCols[0:1]
			resCols = resCols[1:]
			curVals = resVals[0:1]
			resVals = resVals[1:]
		}

		var curStructRef reflect.Value
		if tableName == mainTableStr {
			curStructRef = newStruct

		} else {
			fieldName := mainStructInfo.Fields[tableName].Name
			newStruct.Elem().FieldByName(fieldName).Set(reflect.New(mainStructInfo.Fields[tableName].BaseType))
			curStructRef = newStruct.Elem().FieldByName(fieldName)
		}

		for i, curCol := range curCols {
			if curField, ok := curStructInfo.Fields[curCol]; ok {
				f := curStructRef.Elem().FieldByName(curField.Name)
				curVal := reflect.ValueOf(curVals[i]).Elem()
				if !curVal.IsNil() {

					// TODO: Ugly hack - can it be made prettier?
					// Either it doesn't belong here or a more generic way of
					//   doing this is needed
					if curField.BaseType.String() == "timestamp.Timestamp" {
						newTime := curVal.Elem().Interface().(time.Time)
						newTimestamp := timestamp.Timestamp{
							Seconds: newTime.UTC().Unix(),
							Nanos:   int32(newTime.Nanosecond()),
						}
						f.Set(reflect.ValueOf(&newTimestamp))
					} else {
						f.Set(reflect.Indirect(curVal))

					}

				}
			} else {
				errMsg := fmt.Sprintf("A match wasn't found for the request column: %s.%s", tableName, curCol)
				return nil, errors.New(errMsg)
			}
		}

	}

	return &newStruct, nil
}

func (q *QueryBase) getMainTable() (mainTable Table, err error) {
	// Extract fields info from the query parser
	slct := q.stmt.(*parser.Select).Select.(*parser.SelectClause)

	// Check that there is any FROMs
	if len(slct.From.Tables) == 0 {
		err = errors.New("No FROM found in query")
		return
	}

	// Get the first (left most) TableExpr
	from := slct.From.Tables[0].(parser.TableExpr)

	// If the first expression is a join - get the left most expr from the join
	if expr, ok := from.(*parser.JoinTableExpr); ok {
		from = expr.Left
	}

	// Try to get an *AliasedTableExpr out of the left most from
	if table, ok := from.(Table); ok {
		mainTable = table
	} else {
		err = errors.New("Unable to find the \"main\" table of the query")
	}

	return
}
