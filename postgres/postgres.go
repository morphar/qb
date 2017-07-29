package postgres

import (
	"database/sql"
	"log"
	"strings"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
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
	stmt, err := parser.ParseOne(sql)

	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *parser.Insert:
		q := Insert()
		q.stmt = s
		q.QueryBase.stmt = q.stmt
		return q, nil

	case *parser.Select:
		q := Select()
		q.stmt = s
		q.QueryBase.stmt = q.stmt
		return q, nil

	case *parser.Update:
		q := Update(nil)
		q.stmt = s
		q.QueryBase.stmt = q.stmt
		return q, nil

	case *parser.Delete:
		q := Delete()
		q.stmt = s
		q.QueryBase.stmt = q.stmt
		return q, nil
	}

	return nil, nil
}

func Insert(rows ...interface{}) *InsertQuery {
	insert := &InsertQuery{
		stmt: &parser.Insert{
			Returning: &parser.NoReturningClause{},
		},
	}
	insert.QueryBase.stmt = insert.stmt
	if len(rows) > 0 {
		insert.Insert(rows...)
	}
	return insert
}

func Select(fields ...interface{}) *SelectQuery {
	slct := &SelectQuery{stmt: &parser.Select{}}
	slct.QueryBase.stmt = slct.stmt
	if len(fields) > 0 {
		slct.Select(fields...)
	}
	return slct
}

func Update(tables ...interface{}) *UpdateQuery {
	update := &UpdateQuery{
		stmt: &parser.Update{
			Returning: &parser.NoReturningClause{},
		},
	}
	update.QueryBase.stmt = update.stmt
	if len(tables) == 1 {
		update.Update(tables[0])
	}
	return update
}

func Delete() *DeleteQuery {
	del := &DeleteQuery{
		stmt: &parser.Delete{
			Returning: &parser.NoReturningClause{},
		},
	}
	del.QueryBase.stmt = del.stmt
	return del
}

func (c Column) As(as string) Column {
	c.SelectExpr.As = parser.Name(as)
	return c
}

func (c Column) GetColumn() (str string) {
	if l := len((*c.SelectExpr.Expr.(*parser.UnresolvedName))); l > 0 {
		str = parser.AsString((*c.SelectExpr.Expr.(*parser.UnresolvedName))[l-1])
	}
	return
}

func (c Column) GetTable() (str string) {
	if l := len(*c.SelectExpr.Expr.(*parser.UnresolvedName)); l > 1 {
		str = parser.AsString((*c.SelectExpr.Expr.(*parser.UnresolvedName))[l-2])
	}
	return
}

func (c Column) GetDatabase() (str string) {
	if l := len(c.SelectExpr.Expr.(parser.UnresolvedName)); l > 2 {
		str = parser.AsString((*c.SelectExpr.Expr.(*parser.UnresolvedName))[l-3])
	}
	return
}

func (c Column) String() (str string) {
	return c.SelectExpr.Expr.String()
}

func C(s interface{}) Column {
	var parts []string

	if val, ok := s.(parser.SelectExpr); ok {
		return Column{val}
	} else if val, ok := s.(*parser.UnresolvedName); ok {
		return Column{parser.SelectExpr{Expr: val}}
	} else if val, ok := s.(Column); ok {
		return val
	} else if val, ok := s.(string); ok {
		parts = strings.Split(val, ".")
	}

	c := Column{}

	var col parser.NamePart
	if len(parts) == 3 {
		if parts[2] == "*" {
			col = parser.UnqualifiedStar{}
		} else {
			col = parser.Name(parts[2])
		}

		c.Expr = &parser.UnresolvedName{
			parser.Name(parts[0]), // db
			parser.Name(parts[1]), // table
			col,
		}

	} else if len(parts) == 2 {
		if parts[1] == "*" {
			col = parser.UnqualifiedStar{}
		} else {
			col = parser.Name(parts[1])
		}

		c.Expr = &parser.UnresolvedName{
			parser.Name(parts[0]), // table
			col,
		}

	} else if len(parts) == 1 {
		if parts[0] == "*" {
			col = parser.UnqualifiedStar{}
		} else {
			col = parser.Name(parts[0])
		}

		c.SelectExpr.Expr = parser.UnresolvedName{
			col,
		}
	}
	return c
}

func (t Table) String() (str string) {
	if aliasedTableExpr, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		return aliasedTableExpr.String()
	}

	return
}

/*
func (t Table) GetTable() (str string) {
	if aliasedTableExpr, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		return aliasedTableExpr.String()
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
*/
func (t Table) As(as string) Table {
	if _, ok := t.TableExpr.(*parser.AliasedTableExpr); ok {
		t.TableExpr.(*parser.AliasedTableExpr).As = parser.AliasClause{Alias: parser.Name(as)}
	}
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
