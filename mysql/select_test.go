package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestSelect(t *testing.T) {
	slct := Select("*")
	slct.From("test")
	sql, _ := slct.SQL()
	assert.Equal(t, "select * from test", sql)

	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test", sql)

	sql, err = slct.Select("id").SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select *, id from test", sql)

	sql, err = slct.Select("name").SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select *, id, name from test", sql)

	// TODO: Literal not implemented (Yet?)
	// sql, err = slct.Select(Literal("count(*)").As("count")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select count(*) as count from test", sql)

	// TODO: Literal not implemented (Yet?)
	// slct = Select("*").From("test")
	// sql, err = slct.Select(C("id").As("other_id"), Literal("count(*)").As("count")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select id as other_id, count(*) as count from test", sql)

	// TODO: Sub selects not implemented yet
	// sql, err = slct.From().Select(slct.From("test_1").Select("id")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select (select id from test_1)", sql)

	// TODO: Sub selects not implemented yet
	// slct = Select("*").From("test")
	// sql, err = slct.From().Select(slct.From("test_1").Select("id").As("test_id")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select (select id from test1") as test_id, sql)

	// TODO: Several unimplemented things
	// sql, err = slct.From().
	// 	Select(
	// 		distinct(a).As("distinct"),
	// 		count(a).As("count"),
	// 		L("CASE WHEN ? THEN ? ELSE ? END", MIN(a).Eq(10), true, false),
	// 		L("CASE WHEN ? THEN ? ELSE ? END", AVG(a).Neq(10), true, false),
	// 		L("CASE WHEN ? THEN ? ELSE ? END", FIRST(a).Gt(10), true, false),
	// 		L("CASE WHEN ? THEN ? ELSE ? END", FIRST(a).Gte(10), true, false),
	// 		L("CASE WHEN ? THEN ? ELSE ? END", LAST(a).Lt(10), true, false),
	// 		L("CASE WHEN ? THEN ? ELSE ? END", LAST(a).Lte(10), true, false),
	// 		SUM(a).As("sum"),
	// 		COALESCE(C("a"), a).As("colaseced"),
	// 	).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select distinct(a) as distinct, count(a) as count, CASE WHEN (MIN(a) = 10) THEN true ELSE false END, CASE WHEN (AVG(a) != 10) THEN true ELSE false END, CASE WHEN (FIRST(a) > 10) THEN true ELSE false END, CASE WHEN (FIRST(a) >= 10) THEN true ELSE false END, CASE WHEN (LAST(a) < 10) THEN true ELSE false END, CASE WHEN (LAST(a) <= 10) THEN true ELSE false END, SUM(a) as sum, COALESCE(a, 'a') as colaseced, sql)

	// TODO: Use of struct not implemented, but a good idea!
	// type MyStruct struct {
	// 	Name         string
	// 	Address      string `db:"address"`
	// 	EmailAddress string `db:"email_address"`
	// 	FakeCol      string `db:"-"`
	// }
	// sql, err = slct.Select(&MyStruct{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select "address", "email_address", "name" from test", sql)

	// sql, err = slct.Select(MyStruct{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select "address", "email_address", "name" from test", sql)

	// type myStruct2 struct {
	// 	MyStruct
	// 	Zipcode string `db:"zipcode"`
	// }

	// sql, err = slct.Select(&myStruct2{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select "address", "email_address", "name", "zipcode" from test", sql)

	// sql, err = slct.Select(myStruct2{}).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select "address", "email_address", "name", "zipcode" from test", sql)

	// var myStructs []MyStruct
	// sql, err = slct.Select(&myStructs).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select "address", "email_address", "name" from test", sql)

	// sql, err = slct.Select(myStructs).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select "address", "email_address", "name" from test", sql)

	// //should not change original
	// sql, err = slct.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test", sql)
}

// TODO: // Distinct not implemented yet
// func TestSelectDistinct(t *testing.T) {
// 	slct := Select("*").From("test")

// 	sql, err := slct.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)

// 	sql, err = slct.SelectDistinct("id").SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "id" from test", sql)

// 	sql, err = slct.SelectDistinct("id", "name").SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "id", "name" from test", sql)

// 	sql, err = slct.SelectDistinct(Literal("count(*)").As("count")).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct count(*) as count from test", sql)

// 	sql, err = slct.SelectDistinct(C("id").As("other_id"), Literal("count(*)").As("count")).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "id" as other_id, count(*) as count from test", sql)

// 	type MyStruct struct {
// 		Name         string
// 		Address      string `db:"address"`
// 		EmailAddress string `db:"email_address"`
// 		FakeCol      string `db:"-"`
// 	}
// 	sql, err = slct.SelectDistinct(&MyStruct{}).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "address", "email_address", "name" from test", sql)

// 	sql, err = slct.SelectDistinct(MyStruct{}).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "address", "email_address", "name" from test", sql)

// 	type myStruct2 struct {
// 		MyStruct
// 		Zipcode string `db:"zipcode"`
// 	}

// 	sql, err = slct.SelectDistinct(&myStruct2{}).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "address", "email_address", "name", "zipcode" from test", sql)

// 	sql, err = slct.SelectDistinct(myStruct2{}).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "address", "email_address", "name", "zipcode" from test", sql)

// 	var myStructs []MyStruct
// 	sql, err = slct.SelectDistinct(&myStructs).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "address", "email_address", "name" from test", sql)

// 	sql, err = slct.SelectDistinct(myStructs).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select distinct "address", "email_address", "name" from test", sql)

// 	//should not change original
// 	sql, err = slct.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)

// 	//should not change original
// 	sql, err = slct.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)
// }

// Not implemnted yet
// func TestClearSelect(t *testing.T) {
// 	slct := Select("*").From("test")

// 	sql, err := slct.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)

// 	b := slct.Select(a).ClearSelect()
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)
// }

// Not implemnted yet
// func TestSelectAppend(t *testing.T) {
// 	slct := Select("*").From("test")

// 	sql, err := slct.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)

// 	b := slct.Select(a).SelectAppend(b).SelectAppend("c")
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select a, b, "c" from test", sql)
// }

func TestSelectFrom(t *testing.T) {
	slct := Select("*").From("test")

	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test", sql)

	ds2 := slct.From("test2")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test2", sql)

	ds2 = slct.From("test2", "test3")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test2, test3", sql)

	ds2 = slct.From(T("test2").As("test_2"), "test3")
	sql, err = ds2.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test2 as test_2, test3", sql)

	// TODO: Sub selects not implemented yet
	// ds2 = slct.From(slct.From("test2"), "test3")
	// sql, err = ds2.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from (select * from test) as t1, test3", sql)

	// TODO: Sub selects not implemented yet
	// ds2 = slct.From(slct.From("test2").As("test_2"), "test3")
	// sql, err = ds2.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from (select * from test) as test_2, test3", sql)

	//should not change original
	// sql, err = slct.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test", sql)
}

func TestSelectEmptyWhere(t *testing.T) {
	slct := Select("*").From("test")

	b := slct.Where()
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test", sql)
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
	assert.Equal(t, "select * from test where a is true and a is not true and a is false and a is not false", sql)

	slct = Select("*").From("test")
	slct.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	sql, err = slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test where a = 'a' and b != 'b' and c > 'c' and d >= 'd' and e < 'e' and f <= 'f'", sql)

	// Not implemnted yet
	// b = slct.Where(
	// 	C("a").Eq(From("test2").Select("id")),
	// )
	// sql, err = b.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test where (a IN (select "id" from test"))", sql)

	// slct = Select("*").From("test")
	// slct.Where(Ex{
	// 	"a": "a",
	// 	"b": Op{"neq": "b"},
	// 	"c": Op{"gt": "c"},
	// 	"d": Op{"gte": "d"},
	// 	"e": Op{"lt": "e"},
	// 	"f": Op{"lte": "f"},
	// })
	// sql, err = slc.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test where ((a = 'a') and (b != 'b') and ("c" > 'c') and (d >= 'd') and ("e" < 'e') and ("f" <= 'f'))", sql)

	// b = slct.Where(Ex{
	// 	a: From("test2").Select("id"),
	// })
	// sql, err = b.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test where (a IN (select "id" from test"))", sql)
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
	assert.Equal(t, "select * from test where x = 0 and y = 1 and z = 2 and a = 'A' and b = 'B'", sql)
	sql, err = b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test where x = 0 and y = 1 and z = 2 and a = 'A' and b = 'B'", sql)
}

// Not implemnted yet
// func TestClearWhere(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(
// 		C("a").Eq(1),
// 	).ClearWhere()
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)
// }

func TestLimit(t *testing.T) {
	slct := Select("*").From("test")

	b := slct.Where(
		C("a").Gt(1),
	).Limit(0, 10)
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test where a > 1 limit 0, 10", sql)

	slct = Select("*").From("test")

	b = slct.Where(
		C("a").Gt(1),
	).Limit(0, 0)
	sql, err = b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test where a > 1", sql)
}

// Not implemnted yet
// func TestLimitAll(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(
// 		C("a").Gt(1),
// 	).LimitAll()
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1) LIMIT ALL", sql)

// 	b = slct.Where(
// 		C("a").Gt(1),
// 	).Limit(0).LimitAll()
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1) LIMIT ALL", sql)
// }

// // Not implemnted yet
// func TestClearLimit(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(
// 		C("a").Gt(1),
// 	).LimitAll().ClearLimit()
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1)", sql)

// 	b = slct.Where(
// 		C("a").Gt(1),
// 	).Limit(10).ClearLimit()
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1)", sql)
// }

// Not implemnted yet
// func TestOffset(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(
// 		C("a").Gt(1),
// 	).Offset(10)
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1) OFFSET 10", sql)

// 	b = slct.Where(
// 		C("a").Gt(1),
// 	).Offset(0)
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1)", sql)
// }

// func TestClearOffset(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(
// 		C("a").Gt(1),
// 	).Offset(10).ClearOffset()
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (a > 1)", sql)
// }

func TestGroupBy(t *testing.T) {
	slct := Select("*").From("test")

	b := slct.Where(
		C("a").Gt(1),
	).GroupBy("created")
	sql, err := b.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test where a > 1 group by created", sql)

	// Not implemnted yet
	// b = slct.Where(
	// 	C("a").Gt(1),
	// ).GroupBy(Literal("created::DATE"))
	// sql, err = b.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test where (a > 1) group by created::DATE", sql)

	// Not implemnted yet
	// slct = Select("*").From("test")
	// slct.Where(
	// 	C("a").Gt(1),
	// ).GroupBy("name", C("created::DATE"))
	// sql, err = slct.SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test where a > 1 group by name, created::DATE", sql)
}

// Not implemnted yet
// func TestHaving(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Having(Ex{
// 		a: Op{"gt": 1},
// 	}).GroupBy("created")
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test group by "created" HAVING (a > 1)", sql)

// 	b = slct.Where(Ex{b: true}).
// 		Having(Ex{a: Op{"gt": 1}}).
// 		GroupBy("created")
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test where (b is true) group by "created" HAVING (a > 1)", sql)

// 	b = slct.Having(Ex{a: Op{"gt": 1}})
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test HAVING (a > 1)", sql)

// 	b = slct.Having(Ex{a: Op{"gt": 1}}).Having(Ex{b: 2})
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test HAVING ((a > 1) and (b = 2))", sql)
// }

func TestOrder(t *testing.T) {
	slct := Select("*").From("test")

	slct.Order(C("a").Asc())
	sql, err := slct.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from test order by a asc", sql)

	// TODO: Literal not implemented (yet?)
	// slct := Select("*").From("test")
	// slct.Order(C("a").Asc(), Literal(`("a" + "b" > 2)`).Asc())
	// sql, err := slct.ToSql()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from test order by a asc, a + b > 2 asc", sql)
}

// Not implemnted yet
// func TestOrderAppend(t *testing.T) {
// 	slct := Select("*").From("test")
// 	b := slct.Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test order by a asc NULLS FIRST, b DESC NULLS LAST", sql)

// 	b = From("test").OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
// 	sql, err = b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test order by a asc NULLS FIRST, b DESC NULLS LAST", sql)

// }

// Not implemnted yet
// func TestClearOrder(t *testing.T) {
// 	slct := Select("*").From("test")
// 	b := slct.Order(C("a").Asc().NullsFirst()).ClearOrder()
// 	sql, err := b.SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from test", sql)
// }

func TestJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.Join(T("players").As("p"), C("p.id").Eq(C("items.playerId"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item join players as p on p.id = items.playerId", sql)

	// TODO: Sub selects not implemented yet
	// slct = Select("*").From("item")
	// sql, err = slct.Join(T("players").As("p"), C("p.id").Eq(C("items.playerId"))).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from item join (select * from playrs) as p on p.id = items.playerId", sql)

	slct = Select("*").From("item")
	sql, err = slct.Join(T("v1.test"), C("v1.test.id").Eq(C("items.playerId"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item join v1.test on v1.test.id = items.playerId", sql)

	// TODO: Using not implemented (yet?)
	// sql, err = slct.Join(T("test"), Using(C("name"), C("common_id"))).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from item inner join test USING ("name", "common_id")", sql)

	// TODO: Using not implemented (yet?)
	// sql, err = slct.Join(T("test"), Using("name", "common_id")).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from item inner join test USING ("name", "common_id")", sql)

}

func TestLeftOuterJoin(t *testing.T) {
	slct := Select("*").From("item")

	sql, err := slct.LeftOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item left outer join categories on categories.categoryId = items.id", sql)

	// TODO: In not implemented properly yet
	// sql, err = slct.LeftOuterJoin(T("categories"), And(C("categories.categoryId").Eq(C("items.id")), C("categories.categoryId").In(1, 2, 3))).SQL()
	// assert.NoError(t, err)
	// assert.Equal(t, "select * from item left outer join categories on ((categories.categoryId = items.id) and (categories.categoryId IN (1, 2, 3)))", sql)

	// sql, err = slct.Where(C("price").Lt(100)).RightOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
}

func TestFullOuterJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.
		FullOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).
		Order(C("stamp").Asc()).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item full outer join categories on categories.categoryId = items.id order by stamp asc", sql)

	slct = Select("*").From("item")
	sql, err = slct.FullOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item full outer join categories on categories.categoryId = items.id", sql)
}

func TestInnerJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.
		InnerJoin(T("b"), C("b.itemsId").Eq(C("items.id"))).
		LeftOuterJoin(T("c"), C("c.b_id").Eq(C("b.id"))).
		SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item inner join b on b.itemsId = items.id left outer join c on c.b_id = b.id", sql)

	slct = Select("*").From("item")
	sql, err = slct.
		InnerJoin(T("b"), C("b.itemsId").Eq(C("items.id"))).
		LeftOuterJoin(T("c"), C("c.b_id").Eq(C("b.id"))).
		SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item inner join b on b.itemsId = items.id left outer join c on c.b_id = b.id", sql)

	slct = Select("*").From("item")
	sql, err = slct.InnerJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item inner join categories on categories.categoryId = items.id", sql)
}

func TestRightOuterJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.RightOuterJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item right outer join categories on categories.categoryId = items.id", sql)
}

func TestLeftJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.LeftJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item left join categories on categories.categoryId = items.id", sql)
}

func TestRightJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.RightJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item right join categories on categories.categoryId = items.id", sql)
}

func TestFullJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.FullJoin(T("categories"), C("categories.categoryId").Eq(C("items.id"))).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item full join categories on categories.categoryId = items.id", sql)
}

func TestNaturalJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item natural join categories", sql)
}

func TestNaturalLeftJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalLeftJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item natural left join categories", sql)

}

func TestNaturalRightJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalRightJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item natural right join categories", sql)
}

func TestNaturalFullJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.NaturalFullJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item natural full join categories", sql)
}

func TestCrossJoin(t *testing.T) {
	slct := Select("*").From("item")
	sql, err := slct.From("item").CrossJoin(T("categories")).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "select * from item cross join categories", sql)
}

// TODO: Having not implemented yet
// func TestSqlFunctionExpressionsInHaving(t *testing.T) {
// 	slct := Select("*").From("items")
// 	sql, err := slct.GroupBy("name").Having(SUM("amount").Gt(0)).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from item group by "name" HAVING (SUM("amount") > 0)", sql)
// }

// TODO: Union not implemented yet
// func TestUnion(t *testing.T) {
// 	a := Select("*").From("invoice")
// 	b := Select("*").From("invoice")
// 	a.Select("id", "amount").Where(C("amount").Gt(1000))
// 	b.Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, err := a.Union(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Limit(1).Union(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 UNION (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Order(C("id").Asc()).Union(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) order by "id" asc) as t1 UNION (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Union(b.Limit(1)).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION (select * from (select "id", "amount" from invoce" where ("amount" < 10) LIMIT 1) as t1)", sql)

// 	sql, err = a.Union(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)

// 	sql, err = a.Limit(1).Union(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 UNION (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)

// 	sql, err = a.Union(b).Union(b.Where(C("id").Lt(50))).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION (select "id", "amount" from invoce" where ("amount" < 10)) UNION (select "id", "amount" from invoce" where (("amount" < 10) and ("id" < 50)))", sql)

// }

// TODO: Union not implemented yet
// func TestUnionAll(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, err := a.UnionAll(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION ALL (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Limit(1).UnionAll(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 UNION ALL (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Order(C("id").Asc()).UnionAll(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) order by "id" asc) as t1 UNION ALL (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.UnionAll(b.Limit(1)).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION ALL (select * from (select "id", "amount" from invoce" where ("amount" < 10) LIMIT 1) as t1)", sql)

// 	sql, err = a.UnionAll(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) UNION ALL (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)

// 	sql, err = a.Limit(1).UnionAll(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 UNION ALL (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)
// }

// TODO: Intersect not implemented yet
// func TestIntersect(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, err := a.Intersect(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) INTERSECT (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Limit(1).Intersect(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 INTERSECT (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Order(C("id").Asc()).Intersect(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) order by "id" asc) as t1 INTERSECT (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Intersect(b.Limit(1)).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) INTERSECT (select * from (select "id", "amount" from invoce" where ("amount" < 10) LIMIT 1) as t1)", sql)

// 	sql, err = a.Intersect(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) INTERSECT (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)

// 	sql, err = a.Limit(1).Intersect(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 INTERSECT (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)
// }

// TODO: Intersect not implemented yet
// func TestIntersectAll(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, err := a.IntersectAll(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) INTERSECT ALL (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Limit(1).IntersectAll(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 INTERSECT ALL (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.Order(C("id").Asc()).IntersectAll(b).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) order by "id" asc) as t1 INTERSECT ALL (select "id", "amount" from invoce" where ("amount" < 10))", sql)

// 	sql, err = a.IntersectAll(b.Limit(1)).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) INTERSECT ALL (select * from (select "id", "amount" from invoce" where ("amount" < 10) LIMIT 1) as t1)", sql)

// 	sql, err = a.IntersectAll(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > 1000) INTERSECT ALL (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)

// 	sql, err = a.Limit(1).IntersectAll(b.Order(C("id").Desc())).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > 1000) LIMIT 1) as t1 INTERSECT ALL (select * from (select "id", "amount" from invoce" where ("amount" < 10) order by "id" DESC) as t1)", sql)
// }

//TO PREPARED

// TODO: Prepared not implemented yet
// func TestPreparedWhere(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(Ex{
// 		a: true,
// 		b: Op{"neq": true},
// 		"c": false,
// 		d: Op{"neq": false},
// 		"e": nil,
// 	})
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{})
// 	assert.Equal(t, "select * from test where ((a is true) and (b is not true) and ("c" is false) and (d is not false) and ("e" is NULL))", sql)

// 	b = slct.Where(Ex{
// 		a: a,
// 		b: Op{"neq": b},
// 		"c": Op{"gt": "c"},
// 		d: Op{"gte": d},
// 		"e": Op{"lt": "e"},
// 		"f": Op{"lte": "f"},
// 		"g": Op{"is": nil},
// 		"h": Op{"isnot": nil},
// 	})
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{a, b, "c", d, "e", "f"})
// 	assert.Equal(t, "select * from test where ((a = ?) and (b != ?) and ("c" > ?) and (d >= ?) and ("e" < ?) and ("f" <= ?) and ("g" is NULL) and ("h" is not NULL))", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedLimit(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(C("a").Gt(1)).Limit(10)
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1, 10})
// 	assert.Equal(t, "select * from test where (a > ?) LIMIT ?", sql)

// 	b = slct.Where(C("a").Gt(1)).Limit(0)
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?)", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedLimitAll(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(C("a").Gt(1)).LimitAll()
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?) LIMIT ALL", sql)

// 	b = slct.Where(C("a").Gt(1)).Limit(0).LimitAll()
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?) LIMIT ALL", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedClearLimit(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(C("a").Gt(1)).LimitAll().ClearLimit()
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?)", sql)

// 	b = slct.Where(C("a").Gt(1)).Limit(10).ClearLimit()
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?)", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedOffset(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(C("a").Gt(1)).Offset(10)
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1, 10})
// 	assert.Equal(t, "select * from test where (a > ?) OFFSET ?", sql)

// 	b = slct.Where(C("a").Gt(1)).Offset(0)
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?)", sql)
// }

// TODO: Prepared not implemented yet
// func TestPreparedClearOffset(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(C("a").Gt(1)).Offset(10).ClearOffset()
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?)", sql)
// }

// func TestPreparedGroupBy(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Where(C("a").Gt(1)).GroupBy("created")
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?) group by "created", sql)

// 	b = slct.Where(C("a").Gt(1)).GroupBy(Literal("created::DATE"))
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?) group by created::DATE", sql)

// 	b = slct.Where(C("a").Gt(1)).GroupBy("name", Literal("created::DATE"))
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (a > ?) group by "name", created::DATE", sql)
// }

// func TestPreparedHaving(t *testing.T) {
// 	slct := Select("*").From("test")

// 	b := slct.Having(C("a").Gt(1)).GroupBy("created")
// 	sql, args, err := b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test group by "created" HAVING (a > ?)", sql)

// 	b = slct.
// 		Where(C("b").IsTrue()).
// 		Having(C("a").Gt(1)).
// 		GroupBy("created")
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test where (b is true) group by "created" HAVING (a > ?)", sql)

// 	b = slct.Having(C("a").Gt(1))
// 	sql, args, err = b.Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1})
// 	assert.Equal(t, "select * from test HAVING (a > ?)", sql)
// }

// func TestPreparedJoin(t *testing.T) {
// 	slct := Select("*").From("items")

// 	sql, args, err := slct.Join(T("players").As("p"), On(C("p.id").Eq(C("items.playerId")))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{})
// 	assert.Equal(t, "select * from item inner join players as p on ("p"."id" = "items"."playerId")", sql)

// 	sql, args, err = slct.Join(slct.From("players").As("p"), On(C("p.id").Eq(C("items.playerId")))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{})
// 	assert.Equal(t, "select * from item inner join (select * from playrs") as p on ("p"."id" = "items"."playerId")", sql)

// 	sql, args, err = slct.Join(T("v1").Table("test"), On(C("v1.test.id").Eq(C("items.playerId")))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{})
// 	assert.Equal(t, "select * from item inner join v1."test" on ("v1"."test"."id" = "items"."playerId")", sql)

// 	sql, args, err = slct.Join(T("test"), Using(C("name"), C("common_id"))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{})
// 	assert.Equal(t, "select * from item inner join test USING ("name", "common_id")", sql)

// 	sql, args, err = slct.Join(T("test"), Using("name", "common_id")).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{})
// 	assert.Equal(t, "select * from item inner join test USING ("name", "common_id")", sql)

// 	sql, args, err = slct.Join(T("categories"), On(C("categories.categoryId").Eq(C("items.id")), C("categories.categoryId").In(1, 2, 3))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1, 2, 3})
// 	assert.Equal(t, "select * from item inner join categories on ((categories.categoryId = items.id) and (categories.categoryId IN (?, ?, ?)))", sql)

// }

// func TestPreparedFunctionExpressionsInHaving(t *testing.T) {
// 	slct := Select("*").From("items")
// 	sql, args, err := slct.GroupBy("name").Having(SUM("amount").Gt(0)).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{0})
// 	assert.Equal(t, "select * from item group by "name" HAVING (SUM("amount") > ?)", sql)
// }

// func TestPreparedUnion(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, args, err := a.Union(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) UNION (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.Limit(1).Union(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 1, 10})
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > ?) LIMIT ?) as t1 UNION (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.Union(b.Limit(1)).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10, 1})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) UNION (select * from (select "id", "amount" from invoce" where ("amount" < ?) LIMIT ?) as t1)", sql)

// 	sql, args, err = a.Union(b).Union(b.Where(C("id").Lt(50))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10, 10, 50})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) UNION (select "id", "amount" from invoce" where ("amount" < ?)) UNION (select "id", "amount" from invoce" where (("amount" < ?) and ("id" < ?)))", sql)

// }

// func TestPreparedUnionAll(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, args, err := a.UnionAll(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) UNION ALL (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.Limit(1).UnionAll(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 1, 10})
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > ?) LIMIT ?) as t1 UNION ALL (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.UnionAll(b.Limit(1)).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10, 1})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) UNION ALL (select * from (select "id", "amount" from invoce" where ("amount" < ?) LIMIT ?) as t1)", sql)

// 	sql, args, err = a.UnionAll(b).UnionAll(b.Where(C("id").Lt(50))).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10, 10, 50})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) UNION ALL (select "id", "amount" from invoce" where ("amount" < ?)) UNION ALL (select "id", "amount" from invoce" where (("amount" < ?) and ("id" < ?)))", sql)
// }

// func TestPreparedIntersect(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, args, err := a.Intersect(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) INTERSECT (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.Limit(1).Intersect(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 1, 10})
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > ?) LIMIT ?) as t1 INTERSECT (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.Intersect(b.Limit(1)).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10, 1})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) INTERSECT (select * from (select "id", "amount" from invoce" where ("amount" < ?) LIMIT ?) as t1)", sql)

// }

// func TestPreparedIntersectAll(t *testing.T) {
// 	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
// 	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

// 	sql, args, err := a.IntersectAll(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) INTERSECT ALL (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.Limit(1).IntersectAll(b).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 1, 10})
// 	assert.Equal(t, "select * from (select "id", "amount" from invoce" where ("amount" > ?) LIMIT ?) as t1 INTERSECT ALL (select "id", "amount" from invoce" where ("amount" < ?))", sql)

// 	sql, args, err = a.IntersectAll(b.Limit(1)).Prepared(true).SQL()
// 	assert.NoError(t, err)
// 	assert.Equal(t, args, []interface{}{1000, 10, 1})
// 	assert.Equal(t, "select "id", "amount" from invoce" where ("amount" > ?) INTERSECT ALL (select * from (select "id", "amount" from invoce" where ("amount" < ?) LIMIT ?) as t1)", sql)

// }
