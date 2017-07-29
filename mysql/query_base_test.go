package qb

import (
	"testing"

	parser "github.com/morphar/sqlparsers/pkg/mysql"
	"github.com/stretchr/testify/assert"
)

func TestBaseParse(t *testing.T) {
	// Parse insert
	stmt, err := Parse("INSERT INTO users (fname) VALUES('first')")
	insert := stmt.(*InsertQuery)
	sql, err := insert.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "insert into users(fname) values ('first')", sql)

	// Parse select
	stmt, err = Parse("SELECT * FROM users")
	slct := stmt.(*SelectQuery)
	sql, err = slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from users", sql)

	// Parse update
	stmt, err = Parse("UPDATE users SET lname='last' WHERE id=1")
	update := stmt.(*UpdateQuery)
	sql, err = update.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "update users set lname = 'last' where id = 1", sql)

	// Parse delete
	stmt, err = Parse("DELETE FROM users WHERE id=1")
	del := stmt.(*DeleteQuery)
	sql, err = del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from users where id = 1", sql)

	//
	// Check that we have a link between *Query.Stmt and *Query.QueryBase.Stmt
	//

	// Ensure that insert.Stmt AND insert.QueryBase.Stmt still points to the same thing
	insert.Stmt.Table = newAliasedTableExpr("other", "users").Expr.(parser.TableName)
	stmtSQL := parser.GenerateParsedQuery(insert.Stmt).Query
	stmtBaseSQL := parser.GenerateParsedQuery(insert.QueryBase.Stmt).Query
	assert.Equal(t, "insert into other.users(fname) values ('first')", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)

	// Ensure that slct.Stmt AND slct.QueryBase.Stmt still points to the same thing
	slct.Stmt.From[0] = newAliasedTableExpr("other", "users")
	stmtSQL = parser.GenerateParsedQuery(slct.Stmt).Query
	stmtBaseSQL = parser.GenerateParsedQuery(slct.QueryBase.Stmt).Query
	assert.Equal(t, "select * from other.users", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)

	// Ensure that update.Stmt AND update.QueryBase.Stmt still points to the same thing
	update.Stmt.TableExprs[0] = newAliasedTableExpr("other", "users")
	stmtSQL = parser.GenerateParsedQuery(update.Stmt).Query
	stmtBaseSQL = parser.GenerateParsedQuery(update.QueryBase.Stmt).Query
	assert.Equal(t, "update other.users set lname = 'last' where id = 1", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)

	// Ensure that del.Stmt AND del.QueryBase.Stmt still points to the same thing
	del.Stmt.TableExprs[0] = newAliasedTableExpr("other", "users")
	stmtSQL = parser.GenerateParsedQuery(del.Stmt).Query
	stmtBaseSQL = parser.GenerateParsedQuery(del.QueryBase.Stmt).Query
	assert.Equal(t, "delete from other.users where id = 1", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)
}
