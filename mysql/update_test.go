package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateIsValid(t *testing.T) {
	upd := Update()
	isValid, err := upd.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "No table selected")

	upd.Update("some_table")
	isValid, err = upd.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "Nothing to update")

	upd.Set(C("some").Set("abc"))
	isValid, err = upd.queryIsValid()
	assert.True(t, isValid)
	assert.NoError(t, err)

	sql, err := upd.SQL()
	assert.NoError(t, err)
	assert.NotEmpty(t, sql)
}

func TestUpdate(t *testing.T) {
	upd := Update()
	upd.Update("tbl").Set(C("testcol").Set("123 abc"))
	sql, err := upd.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "update tbl set testcol = '123 abc'", sql)

	upd = Update()
	upd.Update("tbl").Set(C("testcol").Eq("123 abc"))
	sql, err = upd.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "update tbl set testcol = '123 abc'", sql)

	upd = Update()
	upd.Update("tbl1", "tbl2")
	upd.Set(C("tbl1.testcol1").Set(123))
	upd.Set(C("tbl2.testcol2").Eq(321))
	sql, err = upd.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "update tbl1, tbl2 set tbl1.testcol1 = 123, tbl2.testcol2 = 321", sql)
}
