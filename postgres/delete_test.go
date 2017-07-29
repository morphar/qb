package qb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
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
*/
func TestDelete(t *testing.T) {
	del := Delete()
	del.From("test")
	sql, err := del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test", sql)
}

func TestDeleteFrom(t *testing.T) {
	del := Delete().From("test")

	sql, err := del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test", sql)

	ds2 := del.From("test2")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test2", sql)

	ds2 = del.From(T("test2").As("test_2"))
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test2 AS test_2", sql)
}

func TestDeleteEmptyWhere(t *testing.T) {
	del := Delete().From("test")

	b := del.Where()
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test", sql)
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
	assert.Equal(t, "DELETE FROM test WHERE (((a = true) AND (a != true)) AND (a = false)) AND (a != false)", sql)

	del = Delete().From("test")
	del.Where(
		C("a").IsTrue(),
		C("a").IsNotTrue(),
		C("a").IsFalse(),
		C("a").IsNotFalse(),
	)
	sql, err = del.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test WHERE (((a IS true) AND (a IS NOT true)) AND (a IS false)) AND (a IS NOT false)", sql)

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
	assert.Equal(t, "DELETE FROM test WHERE (((((a = 'a') AND (b != 'b')) AND (c > 'c')) AND (d >= 'd')) AND (e < 'e')) AND (f <= 'f')", sql)
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
	assert.Equal(t, "DELETE FROM test WHERE ((((x = 0) AND (y = 1)) AND (z = 2)) AND (a = 'A')) AND (b = 'B')", sql)
	sql, err = b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "DELETE FROM test WHERE ((((x = 0) AND (y = 1)) AND (z = 2)) AND (a = 'A')) AND (b = 'B')", sql)
}
