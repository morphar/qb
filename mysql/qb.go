package qb

import (
	"database/sql"
	"log"
	"strings"

	parser "github.com/morphar/sqlparsers/pkg/mysql"
)

type Query interface {
	SQL() (string, error)
	setDB(*sql.DB)
	queryIsValid() (bool, error)
}

type Errors []error

type Column struct {
	parser.SelectExpr
}

type Table struct {
	parser.TableExpr
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func Parse(sql string) (Query, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *parser.Insert:
		q := Insert()
		q.Stmt = s
		q.QueryBase.Stmt = q.Stmt
		return q, nil

	case *parser.Select:
		q := Select()
		q.Stmt = s
		q.QueryBase.Stmt = q.Stmt
		return q, nil

	case *parser.Update:
		q := Update()
		q.Stmt = s
		q.QueryBase.Stmt = q.Stmt
		return q, nil

	case *parser.Delete:
		q := Delete()
		q.Stmt = s
		q.QueryBase.Stmt = q.Stmt
		return q, nil
	}

	return nil, nil
}

func Insert(rows ...interface{}) *InsertQuery {
	insert := &InsertQuery{Stmt: &parser.Insert{Action: "insert"}}
	insert.QueryBase.Stmt = insert.Stmt
	if len(rows) > 0 {
		insert.Insert(rows...)
	}
	return insert
}

func Select(fields ...interface{}) *SelectQuery {
	slct := &SelectQuery{Stmt: &parser.Select{}}
	slct.QueryBase.Stmt = slct.Stmt
	if len(fields) > 0 {
		slct.Select(fields...)
	}
	return slct
}

func Update(tables ...interface{}) *UpdateQuery {
	update := &UpdateQuery{Stmt: &parser.Update{}}
	update.QueryBase.Stmt = update.Stmt
	if len(tables) > 0 {
		update.Update(tables...)
	}
	return update
}

func Delete() *DeleteQuery {
	del := &DeleteQuery{Stmt: &parser.Delete{}}
	del.QueryBase.Stmt = del.Stmt
	return del
}

func (c Column) As(as string) Column {
	if _, ok := c.SelectExpr.(*parser.AliasedExpr); ok {
		c.SelectExpr.(*parser.AliasedExpr).As = parser.NewColIdent(as)
	}
	return c
}

func (c Column) GetColumn() (str string) {
	if aliasedExpr, ok := c.SelectExpr.(*parser.AliasedExpr); ok {
		colName := aliasedExpr.Expr.(*parser.ColName)
		str = colName.Name.String()
	} else if _, ok := c.SelectExpr.(*parser.StarExpr); ok {
		str = "*"
	}

	return
}

func (c Column) GetTable() (str string) {
	if aliasedExpr, ok := c.SelectExpr.(*parser.AliasedExpr); ok {
		if colName, ok := aliasedExpr.Expr.(*parser.ColName); ok {
			str = colName.Qualifier.Name.String()
		}

	} else if starExpr, ok := c.SelectExpr.(*parser.StarExpr); ok {
		str = starExpr.TableName.Name.String()
	}

	return
}

func (c Column) GetDatabase() (str string) {
	if aliasedExpr, ok := c.SelectExpr.(*parser.AliasedExpr); ok {
		if colName, ok := aliasedExpr.Expr.(*parser.ColName); ok {
			str = colName.Qualifier.Qualifier.String()
		}

	} else if starExpr, ok := c.SelectExpr.(*parser.StarExpr); ok {
		str = starExpr.TableName.Qualifier.String()
	}

	return
}

func (c Column) String() (str string) {
	if aliasedExpr, ok := c.SelectExpr.(*parser.AliasedExpr); ok {
		colName := aliasedExpr.Expr.(*parser.ColName)
		str = colName.Name.String()

		tblName := Table{
			&parser.AliasedTableExpr{
				Expr: colName.Qualifier,
			},
		}

		if tblName.String() != "" {
			str = tblName.String() + "." + str
		}

	} else if starExpr, ok := c.SelectExpr.(*parser.StarExpr); ok {
		str = "*"
		if starExpr.TableName.Name.String() != "" {
			str = starExpr.TableName.Name.String() + "." + str
		}
		if starExpr.TableName.Qualifier.String() != "" {
			str = starExpr.TableName.Qualifier.String() + "." + str
		}
	}

	return
}

func C(s ...interface{}) Column {
	var parts []string

	if len(s) == 1 {
		if val, ok := s[0].(parser.SelectExpr); ok {
			return Column{val}
		} else if val, ok := s[0].(Column); ok {
			return val
		} else if val, ok := s[0].(string); ok {
			parts = strings.Split(val, ".")
		}
	} else {
		// Try to see if this is a string parts. E.g. C("tbl", "col")
		for _, sParam := range s {
			if sStr, ok := sParam.(string); ok {
				parts = append(parts, sStr)
			}
		}
	}

	var db, table, col string

	if len(parts) == 3 {
		db, table, col = parts[0], parts[1], parts[2]

	} else if len(parts) == 2 {
		table, col = parts[0], parts[1]

	} else if len(parts) == 1 {
		col = parts[0]
	}

	if col == "*" {
		return Column{newStarExpr(db, table)}
	} else {
		return Column{newAliasedExpr(db, table, col)}
	}
}

func (t Table) String() (str string) {
	if aliasedTableExpr, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		tblName := aliasedTableExpr.Expr.(parser.TableName)
		if tblName.Qualifier.String() != "" {
			str = tblName.Qualifier.String() + "."
		}
		str += tblName.Name.String()
	}
	// } else if joinTableExpr, ok := t.TableExpr.(*parser.JoinTableExpr); ok {
	// 	_ = joinTableExpr
	// } else if parenTableExpr, ok := t.TableExpr.(*parser.ParenTableExpr); ok {
	// 	_ = parenTableExpr

	return
}

func (t Table) GetTable() (str string) {
	if aliasedTableExpr, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		tblName := aliasedTableExpr.Expr.(parser.TableName)
		str = tblName.Name.String()
	}

	return
}

func (t Table) GetDatabase() (str string) {
	if aliasedTableExpr, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		tblName := aliasedTableExpr.Expr.(parser.TableName)
		str = tblName.Qualifier.String()
	}

	return
}

func (t Table) As(as string) Table {
	if _, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		t.TableExpr.(*parser.AliasedTableExpr).As = parser.NewTableIdent(as)
	}
	return t
}

func T(s ...interface{}) Table {
	var parts []string

	if len(s) == 1 {
		if val, ok := s[0].(*parser.AliasedTableExpr); ok {
			return Table{val}
		} else if val, ok := s[0].(Table); ok {
			return val
		} else if val, ok := s[0].(string); ok {
			parts = strings.Split(val, ".")
		}
	} else {
		// Try to see if this is a string parts. E.g. T("db", "tbl")
		for _, sParam := range s {
			if sStr, ok := sParam.(string); ok {
				parts = append(parts, sStr)
			}
		}
	}

	var db, table string

	if len(parts) == 2 {
		db, table = parts[0], parts[1]

	} else if len(parts) == 1 {
		table = parts[0]
	}

	return Table{newAliasedTableExpr(db, table)}
}

func (e Errors) Error() string {
	str := ""
	for _, err := range e {
		str += err.Error()
	}
	return str
}

func (e Errors) String() string {
	return e.Error()
}
