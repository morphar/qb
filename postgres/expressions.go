package qb

import parser "github.com/morphar/sqlparsers/pkg/postgres"

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

func (c Column) Set(value interface{}) parser.UpdateExpr {
	return newUpdateExpr(c, value)
}

// Order expressions

func (c Column) Asc() *parser.Order {
	return &parser.Order{Expr: c.Expr, Direction: parser.Ascending}
}
func (c Column) Desc() *parser.Order {
	return &parser.Order{Expr: c.Expr, Direction: parser.Descending}
}

// Operator comparison expressions

func (c Column) Eq(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.EQ, c, value)}
}
func (c Column) Neq(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.NE, c, value)}
}
func (c Column) Gt(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.GT, c, value)}
}
func (c Column) Gte(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.GE, c, value)}
}
func (c Column) Lt(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.LT, c, value)}
}
func (c Column) Lte(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.LE, c, value)}
}

// Range expressions

func (c Column) Between(from, to interface{}) RangeCond {
	return RangeCond{
		&parser.RangeCond{
			Not: false,
			// Left: c.Expr,
			// From: convertToExpr(from),
			// To:   convertToExpr(to),
		},
	}
}
func (c Column) NotBetween(from, to interface{}) RangeCond {
	return RangeCond{
		&parser.RangeCond{
			Not: true,
			// Left: c.Expr,
			// From: convertToExpr(from),
			// To:   convertToExpr(to),
		},
	}
}

// TODO: Check that these works!
// List comparison expressions

func (c Column) In(value []interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.In, c, value)}
}
func (c Column) NotIn(value []interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.NotIn, c, value)}
}

// Operator comparison expressions

func (c Column) Like(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.Like, c, value)}
}
func (c Column) ILike(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.ILike, c, value)}
}
func (c Column) NotLike(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.NotLike, c, value)}
}
func (c Column) NotILike(value interface{}) ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.NotILike, c, value)}
}

// These aren't needed, right???
// func (c Column) Is(value interface{}) ComparisonExpr {
// 	return ComparisonExpr{newComparisonExpr("IS", c, value)}
// }
// func (c Column) IsNot(value interface{}) ComparisonExpr {
// 	return ComparisonExpr{newComparisonExpr("IS NOT", c, value)}
// }

// TODO: MAKE SURE THESE WORKS
// Is comparison expressions

func (c Column) IsNull() ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.Is, c, parser.DNull)}
}
func (c Column) IsNotNull() ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.IsNot, c, parser.DNull)}
}
func (c Column) IsTrue() ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.Is, c, parser.DBoolTrue)}
}
func (c Column) IsNotTrue() ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.IsNot, c, parser.DBoolTrue)}
}
func (c Column) IsFalse() ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.Is, c, parser.DBoolFalse)}
}
func (c Column) IsNotFalse() ComparisonExpr {
	return ComparisonExpr{newComparisonExpr(parser.IsNot, c, parser.DBoolFalse)}
}

//

//

//

//

//

//

//

// &parser.ComparisonExpr{
// 			Operator: ">=",
// 			Left: &parser.ColName{
// 				Metadata: nil,
// 				Name:     parser.NewColIdent("id"),
// 				Qualifier: parser.TableName{
// 					Name:      parser.NewTableIdent(""),
// 					Qualifier: parser.NewTableIdent(""),
// 				},
// 			},
// 			Right: &parser.SQLVal{
// 				Type: 1,
// 				Val:  []byte("1"),
// 			},
// 			Escape: nil,
// 		},

/*
type Column struct {
	Database string
	Table    string
	Column   string
}

func C(s string) (c Column) {
	parts := strings.Split(s, ".")

	if len(parts) == 3 {
		c = Column{Database: parts[0], Table: parts[1], Column: parts[2]}

	} else if len(parts) == 2 {
		c = Column{Table: parts[0], Column: parts[1]}

	} else if len(parts) == 1 {
		c = Column{Column: parts[0]}

	}

	return
}

// Comparison functions

type Comparison struct {
	Column   Column
	Operator string      // =, !=, LIKE, etc.
	Value    interface{} // 123, 'abc', Column, etc.
}

type RangeValue struct {
	Start interface{}
	End   interface{}
}

func (c Column) Eq(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "=", Value: value}
}
func (c Column) Neq(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "!=", Value: value}
}
func (c Column) Gt(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: ">", Value: value}
}
func (c Column) Gte(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: ">=", Value: value}
}
func (c Column) Lt(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "<", Value: value}
}
func (c Column) Lte(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "<=", Value: value}
}

func (c Column) In(value []interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "IN", Value: value}
}
func (c Column) NotIn(value []interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "NOT IN", Value: value}
}

func (c Column) Between(value RangeValue) (comp Comparison) {
	return Comparison{Column: c, Operator: "BETWEEN", Value: value}
}
func (c Column) NotBetween(value RangeValue) (comp Comparison) {
	return Comparison{Column: c, Operator: "NOT BETWEEN", Value: value}
}

func (c Column) Like(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "LIKE", Value: value}
}
func (c Column) ILike(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "ILIKE", Value: value}
}
func (c Column) NotLike(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "NOT LIKE", Value: value}
}
func (c Column) NotILike(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "NOT ILIKE", Value: value}
}

func (c Column) Is(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "IS", Value: value}
}
func (c Column) IsNot(value interface{}) (comp Comparison) {
	return Comparison{Column: c, Operator: "IS NOT", Value: value}
}
func (c Column) IsNull() (comp Comparison) {
	return Comparison{Column: c, Operator: "IS NULL"}
}
func (c Column) IsNotNull() (comp Comparison) {
	return Comparison{Column: c, Operator: "IS NOT NULL"}
}
func (c Column) IsTrue() (comp Comparison) {
	return Comparison{Column: c, Operator: "IS TRUE"}
}
func (c Column) IsNotTrue() (comp Comparison) {
	return Comparison{Column: c, Operator: "IS NOT TRUE"}
}
func (c Column) IsFalse() (comp Comparison) {
	return Comparison{Column: c, Operator: "IS FALSE"}
}
func (c Column) IsNotFalse() (comp Comparison) {
	return Comparison{Column: c, Operator: "IS NOT FALSE"}
}

type Table struct {
	Database string
	Table    string
}

func T(s string) (t Table) {
	parts := strings.Split(s, ".")

	if len(parts) >= 2 {
		t.Database = parts[1]
	}
	if len(parts) >= 1 {
		t.Table = parts[0]
	}
	return
}

// (Left AND Right)
type AndExpr struct {
	Left  interface{}
	Right interface{}
}

// (Left OR Right)
type OrExpr struct {
	Left  interface{}
	Right interface{}
}

func And(left, right interface{}) AndExpr {
	return AndExpr{Left: left, Right: right}
}

func Or(left, right interface{}) OrExpr {
	return OrExpr{Left: left, Right: right}
}




*/

// func New(parser string, db *sql.DB) (p QueryBuilder) {
// 	if newParserFunc, ok := parsers[parser]; ok {
// 		return QueryBuilder{
// 			db:     db,
// 			Parser: newParserFunc(),
// 		}
// 	}
// 	return
// }

// func (qb *BaseAdapter) Select(...interface{}) {
// 	// return
// }

// func (a *Parser) Parse(sql string) (s qb.Stmt) {
// 	parserStmt, _ := parser.Parse(sql)
// 	switch stmt := parserStmt.(type) {
// 	case *parser.Insert:
// 		insertStmt := Insert{Insert: *stmt}
// 		s = &insertStmt
// 	case *parser.Select:
// 		selectStmt := Select{Select: *stmt}
// 		s = &selectStmt
// 	case *parser.Update:
// 		updateStmt := Update{Update: *stmt}
// 		s = &updateStmt
// 	case *parser.Delete:
// 		deleteStmt := Delete{Delete: *stmt}
// 		s = &deleteStmt
// 	}
// 	return
// }

// func Select() (s Stmt) {
// 	s = &SelectStmt{}
// 	return
// }

// type Query struct {
// }

// func Select() (q *Stmt) {
// 	bla := parser.Parse("123")

// 	return
// }

// func Insert() (q *Qeury) {
// }

/*
	clauses struct {
		Select         ColumnList
		SelectDistinct ColumnList
		From           ColumnList
		Joins          JoiningClauses
		Where          ExpressionList
		Alias          IdentifierExpression
		GroupBy        ColumnList
		Having         ExpressionList
		Order          ColumnList
		Limit          interface{}
		Offset         uint
		Returning      ColumnList
		Compounds      []CompoundExpression
	}

*/

// func dbConnect() (db *sql.DB, err error) {
// 	dbConf := mysql.Config{
// 		User:   "root",
// 		Passwd: "localhostpass",
// 		Net:    "tcp",
// 		Addr:   "127.0.0.1:8806", // The port will be added later
// 		DBName: "cobiro",
// 		Params: map[string]string{
// 			"parseTime": "true",
// 			// "interpolateParams": "true",
// 		},
// 		// Params           map[string]string // Connection parameters
// 		// Collation        string            // Connection collation
// 		// Loc              *time.Location    // Location for time.Time values
// 	}
// 	dsn := dbConf.FormatDSN()

// 	db, err = sql.Open("mysql", dsn)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	db.SetMaxOpenConns(50)

// 	// Make sure we have a connection
// 	// The db container can take some time to get started, so we'll keep trying
// 	//   every 5 seconds for 5 minutes
// 	endTime := time.Now().Add(5 * time.Minute)
// 	for time.Now().Before(endTime) {
// 		if err = db.Ping(); err == nil {
// 			break
// 		}
// 		time.Sleep(5 * time.Second)
// 	}
// 	return
// }
