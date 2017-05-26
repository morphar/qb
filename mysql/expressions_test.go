package qb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpressionsC(t *testing.T) {
	assert.Equal(t, "*", C("*").String())
	assert.Equal(t, "tbl.*", C("tbl.*").String())
	assert.Equal(t, "db.tbl.*", C("db.tbl.*").String())

	assert.Equal(t, "id", C("id").String())
	assert.Equal(t, "tbl.id", C("tbl.id").String())
	assert.Equal(t, "db.tbl.id", C("db.tbl.id").String())
}

func TestExpressionsT(t *testing.T) {
	assert.Equal(t, "tbl", T("tbl").String())
	assert.Equal(t, "db.tbl", T("db.tbl").String())
}
