package qb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
func TestSelectIsValid(t *testing.T) {
	slct := Select()
	isValid, err := slct.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "No columns selected")

	slct.Select("id", "name")
	isValid, err = slct.queryIsValid()
	assert.False(t, isValid)
	assert.EqualError(t, err, "No table selected")

	slct.From("some_table")
	isValid, err = slct.queryIsValid()
	assert.True(t, isValid)
	assert.NoError(t, err)

	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.NotEmpty(t, sql)
}
*/

func TestSelect(t *testing.T) {
	slct := Select("*")
	slct.From("test")
	sql, _ := slct.SQL()
	assert.Equal(t, "SELECT * FROM test", sql)

	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test", sql)

	sql, err = slct.Select("id").SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT *, id FROM test", sql)

	sql, err = slct.Select("name").SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT *, id, \"name\" FROM test", sql)

	// TODO: Literal not implemented (Yet?)
	// sql, err = slct.Select(Literal("count(*)").As("count")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT count(*) AS count FROM test", sql)

	// TODO: Literal not implemented (Yet?)
	// slct = Select("*").From("test")
	// sql, err = slct.Select(C("id").As("other_id"), Literal("count(*)").As("count")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT id AS other_id, count(*) AS count FROM test", sql)

	// TODO: Sub selects not implemented yet
	// sql, err = slct.From().Select(slct.From("test_1").Select("id")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT (SELECT id FROM test_1)", sql)

	// TODO: Sub selects not implemented yet
	// slct = Select("*").From("test")
	// sql, err = slct.From().Select(slct.From("test_1").Select("id").As("test_id")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT (SELECT id FROM test1") AS test_id, sql)

	// TODO: Several unimplemented things
	// sql, err = slct.From().
	//  Select(
	//      distinct(a).As("distinct"),
	//      count(a).As("count"),
	//      L("CASE WHEN ? THEN ? ELSE ? END", MIN(a).Eq(10), true, false),
	//      L("CASE WHEN ? THEN ? ELSE ? END", AVG(a).Neq(10), true, false),
	//      L("CASE WHEN ? THEN ? ELSE ? END", FIRST(a).Gt(10), true, false),
	//      L("CASE WHEN ? THEN ? ELSE ? END", FIRST(a).Gte(10), true, false),
	//      L("CASE WHEN ? THEN ? ELSE ? END", LAST(a).Lt(10), true, false),
	//      L("CASE WHEN ? THEN ? ELSE ? END", LAST(a).Lte(10), true, false),
	//      SUM(a).As("sum"),
	//      COALESCE(C("a"), a).As("colaseced"),
	//  ).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT distinct(a) AS distinct, count(a) AS count, CASE WHEN (MIN(a) = 10) THEN true ELSE false END, CASE WHEN (AVG(a) != 10) THEN true ELSE false END, CASE WHEN (FIRST(a) > 10) THEN true ELSE false END, CASE WHEN (FIRST(a) >= 10) THEN true ELSE false END, CASE WHEN (LAST(a) < 10) THEN true ELSE false END, CASE WHEN (LAST(a) <= 10) THEN true ELSE false END, SUM(a) AS sum, COALESCE(a, 'a') AS colaseced, sql)

	// TODO: Use of struct not implemented, but a good idea!
	// type MyStruct struct {
	//  Name         string
	//  Address      string `db:"address"`
	//  EmailAddress string `db:"email_address"`
	//  FakeCol      string `db:"-"`
	// }
	// sql, err = slct.Select(&MyStruct{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT "address", "email_address", "name" FROM test", sql)

	// sql, err = slct.Select(MyStruct{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT "address", "email_address", "name" FROM test", sql)

	// type myStruct2 struct {
	//  MyStruct
	//  Zipcode string `db:"zipcode"`
	// }

	// sql, err = slct.Select(&myStruct2{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT "address", "email_address", "name", "zipcode" FROM test", sql)

	// sql, err = slct.Select(myStruct2{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT "address", "email_address", "name", "zipcode" FROM test", sql)

	// var myStructs []MyStruct
	// sql, err = slct.Select(&myStructs).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT "address", "email_address", "name" FROM test", sql)

	// sql, err = slct.Select(myStructs).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT "address", "email_address", "name" FROM test", sql)

	// //should not change original
	// sql, err = slct.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test", sql)
}

// TODO: // Distinct not implemented yet
// func TestSelectDistinct(t *testing.T) {
//  slct := Select("*").From("test")

//  sql, err := slct.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)

//  sql, err = slct.SelectDistinct("id").SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "id" FROM test", sql)

//  sql, err = slct.SelectDistinct("id", "name").SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "id", "name" FROM test", sql)

//  sql, err = slct.SelectDistinct(Literal("count(*)").As("count")).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct count(*) AS count FROM test", sql)

//  sql, err = slct.SelectDistinct(C("id").As("other_id"), Literal("count(*)").As("count")).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "id" AS other_id, count(*) AS count FROM test", sql)

//  type MyStruct struct {
//      Name         string
//      Address      string `db:"address"`
//      EmailAddress string `db:"email_address"`
//      FakeCol      string `db:"-"`
//  }
//  sql, err = slct.SelectDistinct(&MyStruct{}).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "address", "email_address", "name" FROM test", sql)

//  sql, err = slct.SelectDistinct(MyStruct{}).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "address", "email_address", "name" FROM test", sql)

//  type myStruct2 struct {
//      MyStruct
//      Zipcode string `db:"zipcode"`
//  }

//  sql, err = slct.SelectDistinct(&myStruct2{}).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "address", "email_address", "name", "zipcode" FROM test", sql)

//  sql, err = slct.SelectDistinct(myStruct2{}).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "address", "email_address", "name", "zipcode" FROM test", sql)

//  var myStructs []MyStruct
//  sql, err = slct.SelectDistinct(&myStructs).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "address", "email_address", "name" FROM test", sql)

//  sql, err = slct.SelectDistinct(myStructs).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT distinct "address", "email_address", "name" FROM test", sql)

//  //should not change original
//  sql, err = slct.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)

//  //should not change original
//  sql, err = slct.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)
// }

// Not implemnted yet
// func TestClearSelect(t *testing.T) {
//  slct := Select("*").From("test")

//  sql, err := slct.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)

//  b := slct.Select(a).ClearSelect()
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)
// }

// Not implemnted yet
// func TestSelectAppend(t *testing.T) {
//  slct := Select("*").From("test")

//  sql, err := slct.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)

//  b := slct.Select(a).SelectAppend(b).SelectAppend("c")
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT a, b, "c" FROM test", sql)
// }

func TestSelectFrom(t *testing.T) {
	slct := Select("*").From("test")

	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test", sql)

	ds2 := slct.From("test2")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test2", sql)

	ds2 = slct.From("test2", "test3")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test2, test3", sql)

	ds2 = slct.From(T("test2").As("test_2"), "test3")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test2 AS test_2, test3", sql)

	// TODO: Sub selects not implemented yet
	// ds2 = slct.From(slct.From("test2"), "test3")
	// sql, err = ds2.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM (SELECT * FROM test) AS t1, test3", sql)

	// TODO: Sub selects not implemented yet
	// ds2 = slct.From(slct.From("test2").As("test_2"), "test3")
	// sql, err = ds2.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM (SELECT * FROM test) AS test_2, test3", sql)

	//should not change original
	// sql, err = slct.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test", sql)
}

func TestSelectEmptyWhere(t *testing.T) {
	slct := Select("*").From("test")

	b := slct.Where()
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test", sql)
}

func TestSelectWhere(t *testing.T) {
	slct := Select("*").From("test")
	slct.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE (((a = true) AND (a != true)) AND (a = false)) AND (a != false)", sql)

	slct = Select("*").From("test")
	slct.Where(
		C("a").IsTrue(),
		C("a").IsNotTrue(),
		C("a").IsFalse(),
		C("a").IsNotFalse(),
	)
	sql, err = slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE (((a IS true) AND (a IS NOT true)) AND (a IS false)) AND (a IS NOT false)", sql)

	// Not implemnted yet
	// b = slct.Where(
	//  C("a").Eq(From("test2").Select("id")),
	// )
	// sql, err = b.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test WHERE (a IN (SELECT "id" FROM test"))", sql)

	// slct = Select("*").From("test")
	// slct.Where(Ex{
	//  "a": "a",
	//  "b": Op{"neq": "b"},
	//  "c": Op{"gt": "c"},
	//  "d": Op{"gte": "d"},
	//  "e": Op{"lt": "e"},
	//  "f": Op{"lte": "f"},
	// })
	// sql, err = slc.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test WHERE ((a = 'a') AND (b != 'b') AND ("c" > 'c') AND (d >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))", sql)

	// b = slct.Where(Ex{
	//  a: From("test2").Select("id"),
	// })
	// sql, err = b.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test WHERE (a IN (SELECT "id" FROM test"))", sql)
}

func TestSelectWhereChain(t *testing.T) {
	slct := Select("*").From("test").Where(
		C("x").Eq(0),
		C("y").Eq(1),
	)

	slct2 := slct.Where(
		C("z").Eq(2),
	)

	a := slct2.Where(
		C("a").Eq("A"),
	)
	b := slct2.Where(
		C("b").Eq("B"),
	)
	sql, err := a.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE ((((x = 0) AND (y = 1)) AND (z = 2)) AND (a = 'A')) AND (b = 'B')", sql)
	sql, err = b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE ((((x = 0) AND (y = 1)) AND (z = 2)) AND (a = 'A')) AND (b = 'B')", sql)
}

// Not implemnted yet
// func TestClearWhere(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(
//      C("a").Eq(1),
//  ).ClearWhere()
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)
// }

func TestLimit(t *testing.T) {
	slct := Select("*").From("test")

	b := slct.Where(
		C("a").Gt(1),
	).Limit(0, 10)
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE a > 1 LIMIT 10 OFFSET 0", sql)

	slct = Select("*").From("test")

	b = slct.Where(
		C("a").Gt(1),
	).Limit(0, 0)
	sql, err = b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE a > 1", sql)
}

// Not implemnted yet
// func TestLimitAll(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(
//      C("a").Gt(1),
//  ).LimitAll()
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1) LIMIT ALL", sql)

//  b = slct.Where(
//      C("a").Gt(1),
//  ).Limit(0).LimitAll()
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1) LIMIT ALL", sql)
// }

// // Not implemnted yet
// func TestClearLimit(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(
//      C("a").Gt(1),
//  ).LimitAll().ClearLimit()
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1)", sql)

//  b = slct.Where(
//      C("a").Gt(1),
//  ).Limit(10).ClearLimit()
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1)", sql)
// }

// Not implemnted yet
// func TestOffset(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(
//      C("a").Gt(1),
//  ).Offset(10)
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1) OFFSET 10", sql)

//  b = slct.Where(
//      C("a").Gt(1),
//  ).Offset(0)
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1)", sql)
// }

// func TestClearOffset(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(
//      C("a").Gt(1),
//  ).Offset(10).ClearOffset()
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (a > 1)", sql)
// }

func TestGroupBy(t *testing.T) {
	slct := Select("*").From("test")

	b := slct.Where(
		C("a").Gt(1),
	).GroupBy("created")
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test WHERE a > 1 GROUP BY 'created'", sql)

	// Not implemnted yet
	// b = slct.Where(
	//  C("a").Gt(1),
	// ).GroupBy(Literal("created::DATE"))
	// sql, err = b.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test WHERE (a > 1) GROUP BY created::DATE", sql)

	// Not implemnted yet
	// slct = Select("*").From("test")
	// slct.Where(
	//  C("a").Gt(1),
	// ).GroupBy("name", C("created::DATE"))
	// sql, err = slct.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test WHERE a > 1 GROUP BY name, created::DATE", sql)
}

// Not implemnted yet
// func TestHaving(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Having(Ex{
//      a: Op{"gt": 1},
//  }).GroupBy("created")
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test GROUP BY "created" HAVING (a > 1)", sql)

//  b = slct.Where(Ex{b: true}).
//      Having(Ex{a: Op{"gt": 1}}).
//      GroupBy("created")
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test WHERE (b IS true) GROUP BY "created" HAVING (a > 1)", sql)

//  b = slct.Having(Ex{a: Op{"gt": 1}})
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test HAVING (a > 1)", sql)

//  b = slct.Having(Ex{a: Op{"gt": 1}}).Having(Ex{b: 2})
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test HAVING ((a > 1) AND (b = 2))", sql)
// }

func TestOrder(t *testing.T) {
	slct := Select("*").From("test")

	slct.Order(C("a").Asc())
	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test ORDER BY a ASC", sql)

	// TODO: Literal not implemented (yet?)
	// slct := Select("*").From("test")
	// slct.Order(C("a").Asc(), Literal(`("a" + "b" > 2)`).Asc())
	// sql, err := slct.ToSql()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM test ORDER BY a ASC, a + b > 2 ASC", sql)
}

// Not implemnted yet
// func TestOrderAppend(t *testing.T) {
//  slct := Select("*").From("test")
//  b := slct.Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST", sql)

//  b = From("test").OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
//  sql, err = b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST", sql)

// }

// Not implemnted yet
// func TestClearOrder(t *testing.T) {
//  slct := Select("*").From("test")
//  b := slct.Order(C("a").Asc().NullsFirst()).ClearOrder()
//  sql, err := b.SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM test", sql)
// }

func TestJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.Join(T("players").As("p"), C("p.id").Eq(C("items.playerId"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item JOIN players AS p ON p.id = items.\"playerId\"", sql)

	// TODO: Sub selects not implemented yet
	// slct = Select("*").From("item")
	// sql, err = slct.Join(T("players").As("p"), C("p.id").Eq(C("items.playerId"))).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM item JOIN (SELECT * FROM playrs) AS p ON p.id = items.playerId", sql)

	slct = Select("*").From("item")
	sql, err = slct.Join(T("v1.test"), C("v1.test.id").Eq(C("items.playerId"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item JOIN v1.test ON v1.test.id = items.\"playerId\"", sql)

	// TODO: Using not implemented (yet?)
	// sql, err = slct.Join(T("test"), Using(C("name"), C("common_id"))).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM item INNER JOIN test USING ("name", "common_id")", sql)

	// TODO: Using not implemented (yet?)
	// sql, err = slct.Join(T("test"), Using("name", "common_id")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM item INNER JOIN test USING ("name", "common_id")", sql)

}

func TestLeftOuterJoin(t *testing.T) {
	slct := Select("*").From("item")

	sql, err := slct.LeftOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item LEFT OUTER JOIN categories ON categories.\"categoryId\" = items.id", sql)

	// TODO: In not implemented properly yet
	// sql, err = slct.LeftOuterJoin(T("categories"), And(C("categories.categoryId").Eq(C("items.id")), C("categories.categoryId").In(1, 2, 3))).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "SELECT * FROM item LEFT OUTER JOIN categories ON ((categories.categoryId = items.id) AND (categories.categoryId IN (1, 2, 3)))", sql)

	// sql, err = slct.Where(C("price").Lt(100)).RightOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
}

func TestFullOuterJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.
		FullOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).
		Order(C("stamp").Asc()).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item FULL OUTER JOIN categories ON categories.\"categoryId\" = items.id ORDER BY stamp ASC", sql)

	slct = Select("*").From("item")
	sql, err = slct.FullOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item FULL OUTER JOIN categories ON categories.\"categoryId\" = items.id", sql)
}

func TestInnerJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.
		InnerJoin(T("b"), C("b.itemsId").Eq(C("items.id"))).
		LeftOuterJoin(T("c"), C("c.b_id").Eq(C("b.id"))).
		SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item INNER JOIN b ON b.\"itemsId\" = items.id LEFT OUTER JOIN c ON c.b_id = b.id", sql)

	slct = Select("*").From("item")
	sql, err = slct.
		InnerJoin(T("b"), C("b.itemsId").Eq(C("items.id"))).
		LeftOuterJoin(T("c"), C("c.b_id").Eq(C("b.id"))).
		SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item INNER JOIN b ON b.\"itemsId\" = items.id LEFT OUTER JOIN c ON c.b_id = b.id", sql)

	slct = Select("*").From("item")
	sql, err = slct.InnerJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item INNER JOIN categories ON categories.\"categoryId\" = items.id", sql)
}

func TestRightOuterJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.RightOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item RIGHT OUTER JOIN categories ON categories.\"categoryId\" = items.id", sql)
}

func TestLeftJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.LeftJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item LEFT JOIN categories ON categories.\"categoryId\" = items.id", sql)
}

func TestRightJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.RightJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item RIGHT JOIN categories ON categories.\"categoryId\" = items.id", sql)
}

func TestFullJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.FullJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item FULL JOIN categories ON categories.\"categoryId\" = items.id", sql)
}

func TestNaturalJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item NATURAL JOIN categories", sql)
}

func TestNaturalLeftJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalLeftJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item NATURAL LEFT JOIN categories", sql)

}

func TestNaturalRightJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalRightJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item NATURAL RIGHT JOIN categories", sql)
}

func TestNaturalFullJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalFullJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item NATURAL FULL JOIN categories", sql)
}

func TestCrossJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.From("item").CrossJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM item CROSS JOIN categories", sql)
}

// TODO: Having not implemented yet
// func TestSqlFunctionExpressionsInHaving(t *testing.T) {
//  slct := Select("*").From("items")
//  sql, err := slct.GroupBy("name").Having(SUM("amount").Gt(0)).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM item GROUP BY "name" HAVING (SUM("amount") > 0)", sql)
// }

// TODO: Union not implemented yet
// func TestUnion(t *testing.T) {
//  a := Select("*").From("invoice")
//  b := Select("*").From("invoice")
//  a.Select("id", "amount").Where(C("amount").Gt(1000))
//  b.Select("id", "amount").Where(C("amount").Lt(10))

//  sql, err := a.Union(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Limit(1).Union(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Order(C("id").Asc()).Union(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS t1 UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Union(b.Limit(1)).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) LIMIT 1) AS t1)", sql)

//  sql, err = a.Union(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)

//  sql, err = a.Limit(1).Union(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 UNION (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)

//  sql, err = a.Union(b).Union(b.Where(C("id").Lt(50))).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10)) UNION (SELECT "id", "amount" FROM invoce" WHERE (("amount" < 10) AND ("id" < 50)))", sql)

// }

// TODO: Union not implemented yet
// func TestUnionAll(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, err := a.UnionAll(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Limit(1).UnionAll(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 UNION ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Order(C("id").Asc()).UnionAll(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS t1 UNION ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.UnionAll(b.Limit(1)).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) LIMIT 1) AS t1)", sql)

//  sql, err = a.UnionAll(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)

//  sql, err = a.Limit(1).UnionAll(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)
// }

// TODO: Intersect not implemented yet
// func TestIntersect(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, err := a.Intersect(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) INTERSECT (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Limit(1).Intersect(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 INTERSECT (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Order(C("id").Asc()).Intersect(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS t1 INTERSECT (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Intersect(b.Limit(1)).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) LIMIT 1) AS t1)", sql)

//  sql, err = a.Intersect(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)

//  sql, err = a.Limit(1).Intersect(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)
// }

// TODO: Intersect not implemented yet
// func TestIntersectAll(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, err := a.IntersectAll(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) INTERSECT ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Limit(1).IntersectAll(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 INTERSECT ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.Order(C("id").Asc()).IntersectAll(b).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS t1 INTERSECT ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10))", sql)

//  sql, err = a.IntersectAll(b.Limit(1)).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) LIMIT 1) AS t1)", sql)

//  sql, err = a.IntersectAll(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)

//  sql, err = a.Limit(1).IntersectAll(b.Order(C("id").Desc())).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > 1000) LIMIT 1) AS t1 INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < 10) ORDER BY "id" DESC) AS t1)", sql)
// }

//TO PREPARED

// TODO: Prepared not implemented yet
// func TestPreparedWhere(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(Ex{
//      a: true,
//      b: Op{"neq": true},
//      "c": false,
//      d: Op{"neq": false},
//      "e": nil,
//  })
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{})
//  assert.Equal(t, "SELECT * FROM test WHERE ((a IS true) AND (b IS NOT true) AND ("c" IS false) AND (d IS NOT false) AND ("e" IS NULL))", sql)

//  b = slct.Where(Ex{
//      a: a,
//      b: Op{"neq": b},
//      "c": Op{"gt": "c"},
//      d: Op{"gte": d},
//      "e": Op{"lt": "e"},
//      "f": Op{"lte": "f"},
//      "g": Op{"is": nil},
//      "h": Op{"isnot": nil},
//  })
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{a, b, "c", d, "e", "f"})
//  assert.Equal(t, "SELECT * FROM test WHERE ((a = ?) AND (b != ?) AND ("c" > ?) AND (d >= ?) AND ("e" < ?) AND ("f" <= ?) AND ("g" IS NULL) AND ("h" IS NOT NULL))", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedLimit(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(C("a").Gt(1)).Limit(10)
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1, 10})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) LIMIT ?", sql)

//  b = slct.Where(C("a").Gt(1)).Limit(0)
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?)", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedLimitAll(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(C("a").Gt(1)).LimitAll()
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) LIMIT ALL", sql)

//  b = slct.Where(C("a").Gt(1)).Limit(0).LimitAll()
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) LIMIT ALL", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedClearLimit(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(C("a").Gt(1)).LimitAll().ClearLimit()
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?)", sql)

//  b = slct.Where(C("a").Gt(1)).Limit(10).ClearLimit()
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?)", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedOffset(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(C("a").Gt(1)).Offset(10)
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1, 10})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) OFFSET ?", sql)

//  b = slct.Where(C("a").Gt(1)).Offset(0)
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?)", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedClearOffset(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(C("a").Gt(1)).Offset(10).ClearOffset()
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?)", sql)
// }

// func TestPreparedGroupBy(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Where(C("a").Gt(1)).GroupBy("created")
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) GROUP BY "created", sql)

//  b = slct.Where(C("a").Gt(1)).GroupBy(Literal("created::DATE"))
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) GROUP BY created::DATE", sql)

//  b = slct.Where(C("a").Gt(1)).GroupBy("name", Literal("created::DATE"))
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (a > ?) GROUP BY "name", created::DATE", sql)
// }

// func TestPreparedHaving(t *testing.T) {
//  slct := Select("*").From("test")

//  b := slct.Having(C("a").Gt(1)).GroupBy("created")
//  sql, args, err := b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test GROUP BY "created" HAVING (a > ?)", sql)

//  b = slct.
//      Where(C("b").IsTrue()).
//      Having(C("a").Gt(1)).
//      GroupBy("created")
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test WHERE (b IS true) GROUP BY "created" HAVING (a > ?)", sql)

//  b = slct.Having(C("a").Gt(1))
//  sql, args, err = b.Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1})
//  assert.Equal(t, "SELECT * FROM test HAVING (a > ?)", sql)
// }

// func TestPreparedJoin(t *testing.T) {
//  slct := Select("*").From("items")

//  sql, args, err := slct.Join(T("players").As("p"), On(C("p.id").Eq(C("items.playerId")))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{})
//  assert.Equal(t, "SELECT * FROM item INNER JOIN players AS p ON ("p"."id" = "items"."playerId")", sql)

//  sql, args, err = slct.Join(slct.From("players").As("p"), On(C("p.id").Eq(C("items.playerId")))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{})
//  assert.Equal(t, "SELECT * FROM item INNER JOIN (SELECT * FROM playrs") AS p ON ("p"."id" = "items"."playerId")", sql)

//  sql, args, err = slct.Join(T("v1").Table("test"), On(C("v1.test.id").Eq(C("items.playerId")))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{})
//  assert.Equal(t, "SELECT * FROM item INNER JOIN v1."test" ON ("v1"."test"."id" = "items"."playerId")", sql)

//  sql, args, err = slct.Join(T("test"), Using(C("name"), C("common_id"))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{})
//  assert.Equal(t, "SELECT * FROM item INNER JOIN test USING ("name", "common_id")", sql)

//  sql, args, err = slct.Join(T("test"), Using("name", "common_id")).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{})
//  assert.Equal(t, "SELECT * FROM item INNER JOIN test USING ("name", "common_id")", sql)

//  sql, args, err = slct.Join(T("categories"), On(C("categories.categoryId").Eq(C("items.id")), C("categories.categoryId").In(1, 2, 3))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1, 2, 3})
//  assert.Equal(t, "SELECT * FROM item INNER JOIN categories ON ((categories.categoryId = items.id) AND (categories.categoryId IN (?, ?, ?)))", sql)

// }

// func TestPreparedFunctionExpressionsInHaving(t *testing.T) {
//  slct := Select("*").From("items")
//  sql, args, err := slct.GroupBy("name").Having(SUM("amount").Gt(0)).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{0})
//  assert.Equal(t, "SELECT * FROM item GROUP BY "name" HAVING (SUM("amount") > ?)", sql)
// }

// func TestPreparedUnion(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, args, err := a.Union(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.Limit(1).Union(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 1, 10})
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) LIMIT ?) AS t1 UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.Union(b.Limit(1)).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10, 1})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) UNION (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?) LIMIT ?) AS t1)", sql)

//  sql, args, err = a.Union(b).Union(b.Where(C("id").Lt(50))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10, 10, 50})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) UNION (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?)) UNION (SELECT "id", "amount" FROM invoce" WHERE (("amount" < ?) AND ("id" < ?)))", sql)

// }

// func TestPreparedUnionAll(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, args, err := a.UnionAll(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) UNION ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.Limit(1).UnionAll(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 1, 10})
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) LIMIT ?) AS t1 UNION ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.UnionAll(b.Limit(1)).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10, 1})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?) LIMIT ?) AS t1)", sql)

//  sql, args, err = a.UnionAll(b).UnionAll(b.Where(C("id").Lt(50))).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10, 10, 50})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) UNION ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?)) UNION ALL (SELECT "id", "amount" FROM invoce" WHERE (("amount" < ?) AND ("id" < ?)))", sql)
// }

// func TestPreparedIntersect(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, args, err := a.Intersect(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) INTERSECT (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.Limit(1).Intersect(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 1, 10})
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) LIMIT ?) AS t1 INTERSECT (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.Intersect(b.Limit(1)).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10, 1})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?) LIMIT ?) AS t1)", sql)

// }

// func TestPreparedIntersectAll(t *testing.T) {
//  a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
//  b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

//  sql, args, err := a.IntersectAll(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) INTERSECT ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.Limit(1).IntersectAll(b).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 1, 10})
//  assert.Equal(t, "SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) LIMIT ?) AS t1 INTERSECT ALL (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?))", sql)

//  sql, args, err = a.IntersectAll(b.Limit(1)).Prepared(true).SQL()
//  assert.NoError(t, err)
//  assert.Equal(t, args, []interface{}{1000, 10, 1})
//  assert.Equal(t, "SELECT "id", "amount" FROM invoce" WHERE ("amount" > ?) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM invoce" WHERE ("amount" < ?) LIMIT ?) AS t1)", sql)

// }
