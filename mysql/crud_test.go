package mysql

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCRUDBasicDBTest(t *testing.T) {
	// Start by ensuring, we can actually input and output data
	query := `INSERT INTO users (first_name, last_name, email)
	          VALUES('first', 'last', 'first@last.com')`
	_, err := db.Exec(query)
	assert.NoError(t, err)

	query = `SELECT * FROM users`
	rows, err := db.Query(query)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ID int
		var FirstName, LastName, Email string
		var Created time.Time
		err = rows.Scan(&ID, &FirstName, &LastName, &Email, &Created)
		assert.NoError(t, err)
		assert.Equal(t, 1, ID)
		assert.Equal(t, "first", FirstName)
		assert.Equal(t, "last", LastName)
		assert.Equal(t, "first@last.com", Email)
		assert.True(t, time.Now().After(Created))
		assert.True(t, Created.After(time.Now().Add(-time.Hour)))
	}
}

func TestCRUDBasicDBNULLTest(t *testing.T) {
	// Start by ensuring, we can actually input and output data
	query := `INSERT INTO blog_posts (users_id, title, body)
	          VALUES(1, 'blog post title', NULL)`
	_, err := db.Exec(query)
	assert.NoError(t, err)

	q := New(db)

	usersID := 1
	body := "More Content For Better SEO"
	post := BlogPosts{
		ID:      2,
		UsersID: &usersID,
		Title:   "blog post title 2",
		Body:    &body,
	}

	ins := q.Insert(post).Into("blog_posts")
	sql, err := ins.SQL()
	assert.NoError(t, err)
	expectedSQL := "insert into blog_posts(id, users_id, title, body) " +
		"values (2, 1, 'blog post title 2', 'More Content For Better SEO')"
	assert.Equal(t, expectedSQL, sql)

	res, err := ins.Exec()
	assert.NoError(t, err)
	count, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, 1, int(count))
	id, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, 2, int(id))

	upd := q.Update("blog_posts").Set(C("title").Eq("10 queries to run"))
	upd.Where(C("id").Eq(2))
	res, err = upd.Exec()
	assert.NoError(t, err)
	count, err = res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, 1, int(count))
	id, err = res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, 2, int(id))

	del := q.Delete().From("blog_posts")
	del.Where(C("title").Like("%log post tit%"))
	res, err = del.Exec()
	assert.NoError(t, err)
	count, err = res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, 1, int(count))
	id, err = res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, 2, int(id))

	query = `SELECT * FROM blog_posts`
	rows, err := db.Query(query)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer rows.Close()

	rows.Next()
	var ID int
	var UsersID int
	var Title string
	var Body *string
	var Created time.Time
	err = rows.Scan(&ID, &UsersID, &Title, &Body, &Created)
	assert.NoError(t, err)
	assert.Equal(t, 2, ID)
	assert.Equal(t, 1, UsersID)
	assert.Equal(t, "10 queries to run", Title)
	assert.Equal(t, "More Content For Better SEO", *Body)
	assert.True(t, time.Now().After(Created))
	assert.True(t, Created.After(time.Now().Add(-time.Hour)))

}

func TestCRUD(t *testing.T) {
	// user := Customers{}
	// // colsMap, _ := getColumnMap(cust)
	// // spew.Dump(colsMap)
	// colsMap := createColumnMap(reflect.TypeOf(cust))
	// spew.Dump(colsMap)
	// t.Fatal("Aaaaahhhh....")
	// return

	// db, err := dbConnect("cobiro")

	// select customers.id, partners.* from customers LEFT JOIN partners ON partners.id = customers.partner_id

	var posts []BlogPosts
	q := New(db)
	slct := q.Select("blog_posts.*", "users.*")
	slct.From("blog_posts")
	slct.LeftJoin("users", C("users.id").Eq(C("blog_posts.users_id")))
	sql, err := slct.SQL()
	assert.NoError(t, err)
	expectedQuery := "select blog_posts.*, users.*"
	expectedQuery += " from blog_posts left join users on users.id = blog_posts.users_id"
	assert.Equal(t, expectedQuery, sql)

	err = slct.ScanStructs(&posts)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(posts))

	if len(posts) == 2 {
		assert.True(t, time.Now().After(posts[0].Author.Created))
		assert.True(t, posts[0].Author.Created.After(time.Now().Add(-time.Hour)))

		posts[0].Author.Created = time.Now()
	}

	// We don't want to compare time
	posts[0].Created = nil

	// Pointer values
	usersID := 1
	body := "More Content For Better SEO"

	expected := []BlogPosts{
		BlogPosts{
			ID:      2,
			UsersID: &usersID,
			Title:   "10 queries to run",
			Body:    &body,
			Created: nil,
			Author: &Users{
				ID:        1,
				FirstName: "first",
				LastName:  "last",
				Email:     "first@last.com",
				Created:   posts[0].Author.Created,
				BlogPosts: nil,
			},
		},
	}
	assert.True(t, reflect.DeepEqual(expected, posts))

	// var users []Users
	// slct = q.Select("*")
	// err = slct.From(T("users")).Where(C("id").Eq(1)).ScanStruct(&users)
	// log.Println(err)
	// spew.Dump(users)

}

// type Customers struct {
// 	ID         int     `db:"id"`
// 	Email      *string `col:"email"`
// 	Active     *bool   `col:"active"`
// 	FirstName  string  `db:"first_name"`
// 	LastName   string
// 	PartnersID *int      `protobuf:"varint,1,opt,name=partners_id,proto3,customtype=int" json:"partners_id,omitempty" col:"partners_id" default:"true" nullable:"false"`
// 	Partners   *Partners `protobuf:"bytes,33,opt,name=Partners" json:"partners" col:"partner_id" default:"false" fkey:"id" ftable:"partners" nullable:"false" reltype:"one"`
// }

// type Partners struct {
// 	// Standard column fields
// 	Id          *int       `protobuf:"varint,1,opt,name=id,proto3,customtype=int" json:"id,omitempty" col:"id" default:"true" nullable:"false"`
// 	Name        *string    `protobuf:"bytes,2,opt,name=name,proto3,customtype=string" json:"name,omitempty" col:"name" default:"true" nullable:"false"`
// 	CustomersID *int       `protobuf:"varint,1,opt,name=customers_id,proto3,customtype=int" json:"customers_id,omitempty" col:"customers_id" default:"true" nullable:"false"`
// 	Customers   *Customers `protobuf:"bytes,33,opt,name=Customers" json:"customers" col:"customers_id" default:"false" fkey:"id" ftable:"customers" nullable:"false" reltype:"one"`
// }

// type (
// 	columnData struct {
// 		ColumnName string
// 		Transient  bool
// 		Anonymous  bool
// 		Submap     columnMap
// 		FieldName  string
// 		GoType     reflect.Type
// 	}
// 	columnMap map[string]columnData
// 	CrudExec  struct {
// 		Sql  string
// 		Args []interface{}
// 		err  error
// 	}
// )

// var structMapCache = make(map[interface{}]columnMap)
// var structMapsLock = sync.Mutex{}

// func createColumnMap(t reflect.Type) columnMap {
// 	structMapsLock.Lock()
// 	if cm, ok := structMapCache[t]; ok {
// 		structMapsLock.Unlock()
// 		return cm
// 	}
// 	structMapsLock.Unlock()

// 	structFields := map[string]reflect.Type{}
// 	cm := columnMap{}

// 	n := t.NumField()
// 	for i := 0; i < n; i++ {
// 		f := t.Field(i)

// 		log.Println(f.Name, f.Anonymous, f.Type.Kind())

// 		columnName := f.Tag.Get("db")
// 		if columnName == "" {
// 			columnName = strings.ToLower(f.Name)
// 		}
// 		cm[columnName] = columnData{
// 			ColumnName: columnName,
// 			Transient:  columnName == "-",
// 			Anonymous:  f.Anonymous,
// 			FieldName:  f.Name,
// 			GoType:     f.Type,
// 		}

// 		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
// 			structFields[columnName] = f.Type

// 		} else if f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
// 			structFields[columnName] = f.Type
// 		}

// 	}

// 	structMapsLock.Lock()
// 	if _, ok := structMapCache[t]; !ok {
// 		structMapCache[t] = cm
// 	}
// 	structMapsLock.Unlock()

// 	// From here:
// 	// For each structFields:
// 	// 	 if cache does not exist:
// 	// 	   fetch the map
// 	// 	 else
// 	// 	   use current map
// 	// 	 set cur columnMap[colname].Submap = newStructFieldsMap

// 	if len(structFields) > 0 {
// 		for _, typ := range structFields {
// 			if typ.Kind() == reflect.Ptr {
// 				typ = typ.Elem()
// 			}

// 			if typ.Kind() == reflect.Struct {

// 				if _, ok := structMapCache[typ]; !ok {
// 					scm := createColumnMap(typ)

// 					structMapsLock.Lock()
// 					structMapCache[typ] = scm
// 					structMapsLock.Unlock()

// 					log.Println("Got a map")
// 					spew.Dump(typ)

// 					// structMapsLock.Lock()
// 					// structMapCache[typ] = subColumnMap

// 					// // if tmpCM, ok := structMapCache[typ]; ok {
// 					// // 	tmpCM.Submap = subColumnMap
// 					// // 	structMapCache[typ] = tmpCM
// 					// // }
// 					// structMapsLock.Unlock()

// 				}

// 				// structMapsLock.Lock()
// 				// structMapCache[typ] = subColumnMap
// 				// structMapsLock.Unlock()

// 				// structMapsLock.Lock()
// 				// if tmpCM, ok := structMapCache[typ]; ok {
// 				// 	tmpCM.Submap = subColumnMap
// 				// 	structMapCache[typ] = tmpCM
// 				// }
// 				// structMapsLock.Unlock()

// 			}

// 		}
// 	}

// 	return cm
// }

// func TestCRUD(t *testing.T) {
// 	cust := Customers{}
// 	// colsMap, _ := getColumnMap(cust)
// 	// spew.Dump(colsMap)
// 	colsMap := createColumnMap(reflect.TypeOf(cust))
// 	spew.Dump(colsMap)
// 	t.Fatal("Aaaaahhhh....")
// 	return

// 	db, err := dbConnect("cobiro")

// 	q := New(db)
// 	slct := q.Select("*")

// 	var customers []Customers
// 	err = slct.From(T("customers")).Where(C("id").Eq(1)).ScanStructs(&customers)
// 	log.Println(err)
// 	spew.Dump(customers)

// }

// // func getColumnMap(i interface{}) (columnMap, error) {
// // 	val := reflect.Indirect(reflect.ValueOf(i))
// // 	t, valKind, _ := getTypeInfo(i, val)
// // 	if valKind != reflect.Struct {
// // 		return nil, errors.New(fmt.Sprintf("Cannot SELECT into this type: %v", t))
// // 	}

// // 	structMapsLock.Lock()
// // 	if _, ok := structMapCache[t]; !ok {
// // 		structMapCache[t] = createColumnMap(t)
// // 	}
// // 	structMapsLock.Unlock()
// // 	return structMapCache[t], nil
// // }

// // func getTypeInfo(i interface{}, val reflect.Value) (reflect.Type, reflect.Kind, bool) {
// // 	var t reflect.Type
// // 	isSliceOfPointers := false
// // 	valKind := val.Kind()
// // 	if valKind == reflect.Slice {
// // 		if reflect.ValueOf(i).Kind() == reflect.Ptr {
// // 			t = reflect.TypeOf(i).Elem().Elem()
// // 		} else {
// // 			t = reflect.TypeOf(i).Elem()
// // 		}
// // 		if t.Kind() == reflect.Ptr {
// // 			isSliceOfPointers = true
// // 			t = t.Elem()
// // 		}
// // 		valKind = t.Kind()
// // 	} else {
// // 		t = val.Type()
// // 	}
// // 	return t, valKind, isSliceOfPointers
// // }

// // func createColumnMapOrg(t reflect.Type) columnMap {
// // 	cm, n := columnMap{}, t.NumField()
// // 	var subColMaps []columnMap
// // 	for i := 0; i < n; i++ {
// // 		f := t.Field(i)

// // 		log.Println(f.Name, f.Anonymous, f.Type.Kind())

// // 		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
// // 			if f.Type.Kind() == reflect.Ptr {
// // 				subColMaps = append(subColMaps, createColumnMap(f.Type.Elem()))
// // 			} else {
// // 				subColMaps = append(subColMaps, createColumnMap(f.Type))
// // 			}

// // 		} else if f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
// // 			columnName := f.Tag.Get("db")
// // 			if columnName == "" {
// // 				columnName = strings.ToLower(f.Name)
// // 			}

// // 			if _, ok := structMapCache[t]; ok {
// // 				continue
// // 			}

// // 			f.Type.Elem()
// // 			sub, err := getColumnMap(f.Type.Elem())
// // 			log.Println(err)
// // 			spew.Dump(sub)

// // 			// 	// TODO: Make sure we only do this for 1 level...
// // 			// 	// Otherwise we might end up looping around: customer->partner->customer...

// // 			// 	columnName := f.Tag.Get("db")
// // 			// 	if columnName == "" {
// // 			// 		columnName = strings.ToLower(f.Name)
// // 			// 	}
// // 			// 	cm[columnName] = columnData{
// // 			// 		ColumnName: columnName,
// // 			// 		Transient:  columnName == "-",
// // 			// 		Anonymous:  f.Anonymous,
// // 			// 		FieldName:  f.Name,
// // 			// 		GoType:     f.Type,
// // 			// 	}

// // 		} else {
// // 			columnName := f.Tag.Get("db")
// // 			if columnName == "" {
// // 				columnName = strings.ToLower(f.Name)
// // 			}
// // 			cm[columnName] = columnData{
// // 				ColumnName: columnName,
// // 				Transient:  columnName == "-",
// // 				Anonymous:  f.Anonymous,
// // 				FieldName:  f.Name,
// // 				GoType:     f.Type,
// // 			}
// // 		}
// // 	}
// // 	for _, subCm := range subColMaps {
// // 		for key, val := range subCm {
// // 			if _, ok := cm[key]; !ok {
// // 				cm[key] = val
// // 			}
// // 		}
// // 	}
// // 	return cm
// // }

// // type User struct{
// //     FirstName string `db:"first_name"`
// //     LastName  string `db:"last_name"`
// // }

// // var users []User
// // //SELECT "first_name", "last_name" FROM "user";
// // if err := db.From("user").ScanStructs(&users); err != nil{
// //     fmt.Println(err.Error())
// //     return
// // }
// // fmt.Printf("\n%+v", users)

// // var users []User
// // //SELECT "first_name" FROM "user";
// // if err := db.From("user").Select("first_name").ScanStructs(&users); err != nil{
// //     fmt.Println(err.Error())
// //     return
// // }
// // fmt.Printf("\n%+v", users)

// func dbConnect(dbName string) (db *sql.DB, err error) {
// 	dbConf := mysql.Config{
// 		User:   "root",
// 		Passwd: "localhostpass",
// 		Net:    "tcp",
// 		Addr:   "127.0.0.1:8806", // The port will be added later
// 		DBName: dbName,
// 		Params: map[string]string{
// 			"parseTime": "true",
// 			// "interpolateParams": "true",
// 		},
// 		// Params           map[string]string // Connection parameters
// 		// Collation        string            // Connection collation
// 		// Loc              *time.Location    // Location for time.Time values
// 	}

// 	// dbConf := mysql.Config{
// 	// 	User:   "root",
// 	// 	Passwd: "root",
// 	// 	Net:    "tcp",
// 	// 	Addr:   "127.0.0.1:33066", // The port will be added later
// 	// 	DBName: "cobiro",
// 	// 	Params: map[string]string{
// 	// 		"parseTime": "true",
// 	// 		// "interpolateParams": "true",
// 	// 	},
// 	// 	// Params           map[string]string // Connection parameters
// 	// 	// Collation        string            // Connection collation
// 	// 	// Loc              *time.Location    // Location for time.Time values
// 	// }

// 	dsn := dbConf.FormatDSN()

// 	db, err = sql.Open("mysql", dsn)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	db.Exec("SET sql_mode='ANSI_QUOTES'")
// 	db.Exec("SET CHARACTER SET utf8")

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
