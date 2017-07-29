package postgres

import (
	"testing"

	parser "github.com/morphar/sqlparsers/pkg/postgres"
	"github.com/stretchr/testify/assert"
)

func TestBaseParse(t *testing.T) {
	// Parse insert
	stmt, err := Parse("INSERT INTO users (fname) VALUES('first')")
	insert := stmt.(*InsertQuery)
	sql, err := insert.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO users(fname) VALUES ('first')", sql)

	// Parse select
	stmt, err = Parse("SELECT * FROM users")
	slct := stmt.(*SelectQuery)
	sql, err = slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)

	// Parse update
	stmt, err = Parse("UPDATE users SET lname='last' WHERE id=1")
	update := stmt.(*UpdateQuery)
	sql, err = update.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE users SET lname = 'last' WHERE id = 1", sql)

	// Parse delete
	stmt, err = Parse("DELETE FROM users WHERE id=1")
	del := stmt.(*DeleteQuery)
	sql, err = del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM users WHERE id = 1", sql)

	//
	// Check that we have a link between *Query.stmt and *Query.QueryBase.stmt
	//

	// Ensure that insert.stmt AND insert.QueryBase.stmt still points to the same thing
	insert.stmt.Table = newAliasedTableExpr("other", "users").Expr.(*parser.NormalizableTableName)
	stmtSQL := insert.stmt.String()
	stmtBaseSQL := insert.QueryBase.stmt.String()
	assert.Equal(t, "INSERT INTO other.users(fname) VALUES ('first')", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)

	// Ensure that slct.stmt AND slct.QueryBase.stmt still points to the same thing
	slct.stmt.Select.(*parser.SelectClause).From.Tables[0] = newAliasedTableExpr("other", "users")
	stmtSQL = slct.stmt.String()
	stmtBaseSQL = slct.QueryBase.stmt.String()
	assert.Equal(t, "SELECT * FROM other.users", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)

	// Ensure that update.stmt AND update.QueryBase.stmt still points to the same thing
	update.stmt.Table = T("other.users")
	stmtSQL = update.stmt.String()
	stmtBaseSQL = update.QueryBase.stmt.String()
	assert.Equal(t, "UPDATE other.users SET lname = 'last' WHERE id = 1", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)

	// Ensure that del.stmt AND del.QueryBase.stmt still points to the same thing
	del.stmt.Table = T("other.users")
	stmtSQL = del.stmt.String()
	stmtBaseSQL = del.QueryBase.stmt.String()
	assert.Equal(t, "DELETE FROM other.users WHERE id = 1", stmtSQL)
	assert.Equal(t, stmtSQL, stmtBaseSQL)
}
