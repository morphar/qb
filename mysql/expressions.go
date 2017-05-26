package qb

import parser "github.com/morphar/sqlparsers/mysql"

func And(left, right parser.Expr) *parser.AndExpr {
	return &parser.AndExpr{Left: left, Right: right}
}

func Or(left, right parser.Expr) *parser.OrExpr {
	return &parser.OrExpr{Left: left, Right: right}
}

func Not(expr parser.Expr) *parser.NotExpr {
	return &parser.NotExpr{Expr: expr}
}

type ComparisonExpr struct {
	*parser.ComparisonExpr
}

type RangeCond struct {
	*parser.RangeCond
}

type IsExpr struct {
	*parser.IsExpr
}

func (c Column) Set(value interface{}) parser.UpdateExpr {
	return newUpdateExpr(c, value)
}

// Order expressions

func (c Column) Asc() *parser.Order {
	return &parser.Order{
		Expr:      c.SelectExpr.(*parser.AliasedExpr).Expr,
		Direction: "asc",
	}
}
func (c Column) Desc() *parser.Order {
	return &parser.Order{
		Expr:      c.SelectExpr.(*parser.AliasedExpr).Expr,
		Direction: "desc",
	}
}

// Operator comparison expressions

// TODO: Isn't it more natual/right to have Eq, etc. on an expression, rather
// than forcing the use of a column?
// It might make sense to have a comparator... e.g.: Compare(operator, expr1, expr2)
func (c Column) Eq(value interface{}) ComparisonExpr {
	if v, ok := value.(bool); ok {
		return ComparisonExpr{newComparisonExpr("is", c, v)}
	}

	return ComparisonExpr{newComparisonExpr("=", c, value)}
}
func (c Column) Neq(value interface{}) ComparisonExpr {
	if v, ok := value.(bool); ok {
		return ComparisonExpr{newComparisonExpr("is not", c, v)}
	}

	return ComparisonExpr{newComparisonExpr("!=", c, value)}
}
func (c Column) Gt(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(">", c, value)}
}
func (c Column) Gte(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(">=", c, value)}
}
func (c Column) Lt(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("<", c, value)}
}
func (c Column) Lte(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("<=", c, value)}
}

// Range expressions

func (c Column) Between(from, to interface{}) RangeCond {
	return RangeCond{
		&parser.RangeCond{
			Operator: "between",
			Left:     c.SelectExpr.(*parser.AliasedExpr).Expr,
			From:     convertToExpr(from),
			To:       convertToExpr(to),
		},
	}
}
func (c Column) NotBetween(from, to interface{}) RangeCond {
	return RangeCond{
		&parser.RangeCond{
			Operator: "not between",
			Left:     c.SelectExpr.(*parser.AliasedExpr).Expr,
			From:     convertToExpr(from),
			To:       convertToExpr(to),
		},
	}
}

// TODO: Check that these works!
// List comparison expressions

func (c Column) In(values ...interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("IN", c, values)}
}
func (c Column) NotIn(values ...interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("NOT IN", c, values)}
}

// Operator comparison expressions

func (c Column) Like(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("LIKE", c, value)}
}
func (c Column) ILike(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("ILIKE", c, value)}
}
func (c Column) NotLike(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("NOT LIKE", c, value)}
}
func (c Column) NotILike(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr("NOT ILIKE", c, value)}
}

// TODO: MAKE SURE THESE WORKS
// Is comparison expressions

func (c Column) IsNull() IsExpr {
	return IsExpr{
		&parser.IsExpr{
			Operator: "IS NULL",
			Expr:     c.SelectExpr.(*parser.AliasedExpr).Expr,
		},
	}
}
func (c Column) IsNotNull() IsExpr {
	return IsExpr{
		&parser.IsExpr{
			Operator: "IS NOT NULL",
			Expr:     c.SelectExpr.(*parser.AliasedExpr).Expr,
		},
	}
}
func (c Column) IsTrue() IsExpr {
	return IsExpr{
		&parser.IsExpr{
			Operator: "IS TRUE",
			Expr:     c.SelectExpr.(*parser.AliasedExpr).Expr,
		},
	}
}
func (c Column) IsNotTrue() IsExpr {
	return IsExpr{
		&parser.IsExpr{
			Operator: "IS NOT TRUE",
			Expr:     c.SelectExpr.(*parser.AliasedExpr).Expr,
		},
	}
}
func (c Column) IsFalse() IsExpr {
	return IsExpr{
		&parser.IsExpr{
			Operator: "IS FALSE",
			Expr:     c.SelectExpr.(*parser.AliasedExpr).Expr,
		},
	}
}
func (c Column) IsNotFalse() IsExpr {
	return IsExpr{
		&parser.IsExpr{
			Operator: "IS NOT FALSE",
			Expr:     c.SelectExpr.(*parser.AliasedExpr).Expr,
		},
	}
}
