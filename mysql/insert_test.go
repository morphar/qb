package qb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRow struct {
	ID   int    `json:"id,omitempty" col:"id"`
	Name string `json:"name,omitempty" db:"name"`
}

type testRowPointers struct {
	ID   *int
	Name *string
}

func TestInsertIsValid(t *testing.T) {
	insert := Insert()
	isValid, err := insert.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "No table name provided")

	insert.Into("items")
	isValid, err = insert.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "No values provided")
}

func TestInsertRowColsAndVals(t *testing.T) {
	insert := Insert()
	_, _, err := insert.rowColsAndVals()
	assert.EqualError(t, err, "No rows provided")

	row1 := map[string]interface{}{"some1": 1}
	row2 := testRow{1, "name"}
	_, _, err = insert.rowColsAndVals(row1, row2)
	assert.EqualError(t, err, "Rows must all be of the same type. Expected map[string]interface {} got qb.testRow")

	row1WrongType := map[string]string{"some1": "other1"}
	row2WrongType := map[string]string{"some2": "other2"}
	_, _, err = insert.rowColsAndVals(row1WrongType, row2WrongType)
	assert.EqualError(t, err, "Wrong map type. Expected map[string]interface {} or map[Column]interface {} got map[string]string")

	row1WrongLength := map[string]interface{}{"some1": 1}
	row2WrongLength := map[string]interface{}{"some2": 2, "extra": "wrong"}
	_, _, err = insert.rowColsAndVals(row1WrongLength, row2WrongLength)
	assert.EqualError(t, err, "Rows with different value length. Expected 1 got 2")

	row1WrongKeys := map[string]interface{}{"some1": 1}
	row2WrongKeys := map[string]interface{}{"some2": 2}
	_, _, err = insert.rowColsAndVals(row1WrongKeys, row2WrongKeys)
	assert.EqualError(t, err, "Rows with different keys")

	_, _, err = insert.rowColsAndVals("Yo", "Joe")
	assert.EqualError(t, err, "Wrong type of insert value. Supported types: sub select, map[string]interface{}, map[Column]interface{} and Struct. Got: string")
}

func TestSimpleMapInsert(t *testing.T) {
	rows := []map[string]interface{}{
		map[string]interface{}{"id": 1, "name": "name 1"},
		map[string]interface{}{"id": 2, "name": "name 2"},
		map[string]interface{}{"id": 3, "name": "name 3"},
	}

	insert := Insert(rows[0], rows[1]).Into("items")
	_, err := insert.SQL()
	assert.NoError(t, err)

	insert = Insert(rows).Into("items")
	_, err = insert.SQL()
	assert.NoError(t, err)
}

func TestSimpleStructInsert(t *testing.T) {
	insert := Insert(testRow{1, "name"}, testRow{2, "other name"}).Into("items")
	_, err := insert.SQL()
	assert.NoError(t, err)

	id1 := 1
	name1 := "name"
	row1 := testRowPointers{&id1, &name1}

	id2 := 2
	name2 := "other name"
	row2 := testRowPointers{&id2, &name2}

	insert = Insert(row1, row2).Into("items")
	_, err = insert.SQL()
	assert.NoError(t, err)

	insert = Insert([]testRowPointers{row1, row2}).Into("items")
	_, err = insert.SQL()
	assert.NoError(t, err)
}

// func TestWrongRowTypes(t *testing.T) {
// 	row1 := map[string]interface{}{"some1": 1}
// 	insert := Insert(row1)
// 	insert.Into("items")

// 	// q, p, err := insert.SQL()
// 	_, _, err := insert.SQL()
// 	assert.NoError(t, err)
// }
