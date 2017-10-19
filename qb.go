// TODO: Copy the mysql and postgres QueryBuilder interface and we should be
// halfway there! There rest is a question of making interfaces for the return types...

package qb

type QueryBuilder struct {
}

func New(typ string) {
	// switch strings.ToLower(typ) {
	// case "postgres":
	// 	return NewPostgres()
	// 	break
	// case "mysql", "sqlite":
	// 	return NewMySQL()
	// 	break
	// }
}

func NewPostgres() {

}

func NewMySQL() {

}
