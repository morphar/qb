package qb

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func TestMain(m *testing.M) {
	log.SetFlags(log.Ltime | log.Lshortfile)

	flag.Parse()

	var res int
	defer func() {
		if x := recover(); x != nil {
			log.Printf("run time panic: %v", x)
		}
		teardownDatabase()
		os.Exit(res)
	}()

	err := setupDatabase()
	if err != nil {
		log.Println(err)
		teardownDatabase()
		os.Exit(1)
	}

	res = m.Run()
}

type BlogPosts struct {
	// Standard column fields
	ID      int        `json:"id,omitempty" col:"id" default:"true" nullable:"false"`
	UsersID *int       `json:"usersId,omitempty" col:"users_id" default:"true" fkey:"id" ftable:"users" nullable:"true"`
	Title   string     `json:"title,omitempty" col:"title" default:"true" nullable:"false"`
	Body    *string    `json:"body,omitempty" col:"body" default:"true" nullable:"true"`
	Created *time.Time `json:"created,omitempty" col:"created" default:"true" nullable:"true"`

	// Direct relations
	Author *Users `json:"author,omitempty" col:"users_id" default:"true" fkey:"id" ftable:"users" nullable:"false" reltype:"one"`
}

type Users struct {
	// Standard column fields
	ID        int       `json:"id,omitempty" col:"id" default:"true" nullable:"false"`
	FirstName string    `json:"firstName,omitempty" col:"first_name" default:"true" nullable:"false"`
	LastName  string    `json:"lastName,omitempty" col:"last_name" default:"true" nullable:"false"`
	Email     string    `json:"email,omitempty" col:"email" default:"true" nullable:"false"`
	Created   time.Time `json:"created,omitempty" col:"created" default:"true" nullable:"false"`

	// Indirect relations
	BlogPosts []*BlogPosts `json:"blogPosts,omitempty" col:"blog_posts_id" default:"true" fkey:"id" ftable:"blog_posts" nullable:"false" reltype:"many"`
}

//
// Helpers
//

func setupDatabase() (err error) {
	if db, err = sql.Open("sqlite3", ":memory:"); err != nil {
		return
	}
	err = createTables()
	return
}

func createTables() (err error) {
	query := `
CREATE TABLE blog_posts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	users_id INT NOT NULL DEFAULT '0',
	title VARCHAR NOT NULL,
	body VARCHAR DEFAULT NULL,
	created DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	first_name VARCHAR NOT NULL,
	last_name VARCHAR NOT NULL,
	email VARCHAR NOT NULL,
	created DATETIME DEFAULT CURRENT_TIMESTAMP
);`

	_, err = db.Exec(query)
	return
}

func teardownDatabase() {
	if db != nil {
		db.Close()
	}
	return
}
