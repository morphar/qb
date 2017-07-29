package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteIsValid(t *testing.T) {
	del := Delete()
	isValid, err := del.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "No table selected")

	del.From("some_table")
	isValid, err = del.queryIsValid()
	assert.True(t, isValid)
	assert.NoError(t, err)

	sql, err := del.SQL()
	assert.NoError(t, err)
	assert.NotEmpty(t, sql)
}

func TestDelete(t *testing.T) {
	del := Delete()
	del.From("test")
	sql, err := del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test", sql)
}

func TestDeleteFrom(t *testing.T) {
	del := Delete().From("test")

	sql, err := del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test", sql)

	ds2 := del.From("test2")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test2", sql)

	ds2 = del.From("test2", "test3")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test2, test3", sql)

	ds2 = del.From(T("test2").As("test_2"), "test3")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test2 as test_2, test3", sql)
}

func TestDeleteEmptyWhere(t *testing.T) {
	del := Delete().From("test")

	b := del.Where()
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test", sql)
}

func TestDeleteWhere(t *testing.T) {
	del := Delete().From("test")

	del.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	sql, err := del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test where a is true and a is not true and a is false and a is not false", sql)

	del = Delete().From("test")
	del.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	sql, err = del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test where a = 'a' and b != 'b' and c > 'c' and d >= 'd' and e < 'e' and f <= 'f'", sql)
}

func TestDeleteWhereChain(t *testing.T) {
	del := Delete().From("test").Where(
		C("x").Eq(0),
		C("y").Eq(1),
	)

	del2 := del.Where(
		C("z").Eq(2),
	)

	a := del2.Where(
		C("a").Eq("A"),
	)
	b := del2.Where(
		C("b").Eq("B"),
	)
	sql, err := a.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test where x = 0 and y = 1 and z = 2 and a = 'A' and b = 'B'", sql)
	sql, err = b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "delete from test where x = 0 and y = 1 and z = 2 and a = 'A' and b = 'B'", sql)
}
