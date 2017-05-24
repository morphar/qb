package qb

import (
	"log"
	"strings"

	parser "github.com/morphar/sqlparsers/mysql"
)

// var db *sql.DB

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	// var err error
	// if db, err = dbConnect(); err != nil {
	// 	log.Fatal(err)
	// }
}

func Insert(rows ...interface{}) *InsertQuery {
	insert := &InsertQuery{Stmt: &parser.Insert{Action: "insert"}}
	if len(rows) > 0 {
		insert.Insert(rows...)
	}
	return insert
}

func Select(fields ...interface{}) *SelectQuery {
	slct := &SelectQuery{Stmt: &parser.Select{}}
	if len(fields) > 0 {
		slct.Select(fields...)
	}
	return slct
}

func Update() *UpdateQuery {
	return &UpdateQuery{Stmt: &parser.Update{}}
}

func Delete() *DeleteQuery {
	return &DeleteQuery{Stmt: &parser.Delete{}}
}

type Column struct {
	*parser.AliasedExpr
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

type Table struct {
	*parser.AliasedTableExpr
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

type Errors []error

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
