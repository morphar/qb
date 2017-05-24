package qb

import (
	"database/sql"
	"log"
	"strings"

	parser "github.com/morphar/sqlparsers/mysql"
)

type Query interface {
	SQL() (string, []interface{}, error)
	setDB(*sql.DB)
	queryIsValid() (bool, error)
}

type Errors []error

type Column struct {
	*parser.AliasedExpr
}

type Table struct {
	*parser.AliasedTableExpr
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

func Update() *UpdateQuery {
	update := &UpdateQuery{Stmt: &parser.Update{}}
	update.QueryBase.Stmt = update.Stmt
	return update
}

func Delete() *DeleteQuery {
	del := &DeleteQuery{Stmt: &parser.Delete{}}
	del.QueryBase.Stmt = del.Stmt
	return del
}

func (c Column) As(as string) Column {
	c.AliasedExpr.As = parser.NewColIdent(as)
	return c
}

func (c Column) String() (str string) {
	colName := c.Expr.(*parser.ColName)
	str = colName.Name.String()

	tblName := Table{&parser.AliasedTableExpr{}}
	tblName.Expr = colName.Qualifier
	if tblName.String() != "" {
		str = tblName.String() + "." + str
	}

	return
}

func C(s interface{}) Column {
	var parts []string

	if val, ok := s.(*parser.AliasedExpr); ok {
		return Column{val}
	} else if val, ok := s.(Column); ok {
		return val
	} else if val, ok := s.(string); ok {
		parts = strings.Split(val, ".")
	}

	var db, table, col string

	if len(parts) == 3 {
		db, table, col = parts[0], parts[1], parts[2]

	} else if len(parts) == 2 {
		table, col = parts[0], parts[1]

	} else if len(parts) == 1 {
		col = parts[0]
	}

	return Column{newAliasedExpr(db, table, col)}
}

func (t Table) String() (str string) {
	tblName := t.Expr.(parser.TableName)
	if tblName.Qualifier.String() != "" {
		str = tblName.Qualifier.String() + "."
	}

	str += tblName.Name.String()

	return
}

func (t Table) As(as string) Table {
	t.AliasedTableExpr.As = parser.NewTableIdent(as)
	return t
}

func T(s interface{}) Table {
	var parts []string

	if val, ok := s.(*parser.AliasedTableExpr); ok {
		return Table{val}
	} else if val, ok := s.(Table); ok {
		return val
	} else if val, ok := s.(string); ok {
		parts = strings.Split(val, ".")
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
