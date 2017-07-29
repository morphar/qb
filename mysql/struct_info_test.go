package mysql

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Cobiro/go-utils/tagstring"
	"github.com/stretchr/testify/assert"
)

func TestStructInfoSimple(t *testing.T) {
	usersInfoExpected := StructInfo{
		Name:         "Users",
		DBName:       "users",
		Type:         reflect.TypeOf(Users{}),
		NonRelFields: 5,
		Fields: map[string]FieldInfo{
			"id": FieldInfo{
				Index: 0, Name: "ID", DBName: "id",
				Tag:      tagstring.TagString("json:\"id,omitempty\" col:\"id\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf(0), BaseType: reflect.TypeOf(0),
			},
			"first_name": FieldInfo{
				Index: 1, Name: "FirstName", DBName: "first_name",
				Tag:      tagstring.TagString("json:\"firstName,omitempty\" col:\"first_name\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf("a string"), BaseType: reflect.TypeOf("a string"),
			},
			"last_name": FieldInfo{
				Index: 2, Name: "LastName", DBName: "last_name",
				Tag:      tagstring.TagString("json:\"lastName,omitempty\" col:\"last_name\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf("a string"), BaseType: reflect.TypeOf("a string"),
			},
			"email": FieldInfo{
				Index: 3, Name: "Email", DBName: "email",
				Tag:      tagstring.TagString("json:\"email,omitempty\" col:\"email\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf("a string"), BaseType: reflect.TypeOf("a string"),
			},
			"created": FieldInfo{
				Index: 4, Name: "Created", DBName: "created",
				Tag:      tagstring.TagString("json:\"created,omitempty\" col:\"created\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf(time.Time{}), BaseType: reflect.TypeOf(time.Time{}),
			},
			"blog_posts": FieldInfo{
				Index: 5, Name: "BlogPosts", DBName: "blog_posts",
				Tag:      tagstring.TagString("json:\"blogPosts,omitempty\" col:\"blog_posts_id\" default:\"true\" fkey:\"id\" ftable:\"blog_posts\" nullable:\"false\" reltype:\"many\""),
				IsParent: true, IsSlice: true, RelType: "many",
				Type: reflect.TypeOf([]*BlogPosts{}), BaseType: reflect.TypeOf(BlogPosts{}),
			},
		},
	}

	postsInfoExpected := StructInfo{
		Name:         "BlogPosts",
		DBName:       "blog_posts",
		Type:         reflect.TypeOf(BlogPosts{}),
		NonRelFields: 5,
		Fields: map[string]FieldInfo{
			"id": FieldInfo{
				Index: 0, Name: "ID", DBName: "id",
				Tag:      tagstring.TagString("json:\"id,omitempty\" col:\"id\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf(0), BaseType: reflect.TypeOf(0),
			},
			"users_id": FieldInfo{
				Index: 1, Name: "UsersID", DBName: "users_id",
				Tag:      tagstring.TagString("json:\"usersId,omitempty\" col:\"users_id\" default:\"true\" fkey:\"id\" ftable:\"users\" nullable:\"true\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.New(reflect.TypeOf(0)).Type(), BaseType: reflect.TypeOf(0),
			},
			"title": FieldInfo{
				Index: 2, Name: "Title", DBName: "title",
				Tag:      tagstring.TagString("json:\"title,omitempty\" col:\"title\" default:\"true\" nullable:\"false\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.TypeOf("a string"), BaseType: reflect.TypeOf("a string"),
			},
			"body": FieldInfo{
				Index: 3, Name: "Body", DBName: "body",
				Tag:      tagstring.TagString("json:\"body,omitempty\" col:\"body\" default:\"true\" nullable:\"true\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.New(reflect.TypeOf("a string")).Type(), BaseType: reflect.TypeOf("a string"),
			},
			"created": FieldInfo{
				Index: 4, Name: "Created", DBName: "created",
				Tag:      tagstring.TagString("json:\"created,omitempty\" col:\"created\" default:\"true\" nullable:\"true\""),
				IsParent: false, IsSlice: false, RelType: "",
				Type: reflect.New(reflect.TypeOf(time.Now())).Type(), BaseType: reflect.TypeOf(time.Time{}),
			},
			"users": FieldInfo{
				Index: 5, Name: "Author", DBName: "users",
				Tag:      tagstring.TagString("json:\"author,omitempty\" col:\"users_id\" default:\"true\" fkey:\"id\" ftable:\"users\" nullable:\"false\" reltype:\"one\""),
				IsParent: true, IsSlice: false, RelType: "one",
				Type: reflect.New(reflect.TypeOf(Users{})).Type(), BaseType: reflect.TypeOf(Users{}),
			},
		},
	}

	// Seems like the only reliable (fastest) way is to convert to JSON
	// reflect.DeepEqual works, but sees deifferent pointers to same type as not equal (of course)

	var users Users
	usersInfo := getStructInfo(users)
	assert.EqualValues(t, usersInfoExpected, usersInfo)

	var posts BlogPosts
	postsInfo := getStructInfo(posts)
	postsJSON, err := json.Marshal(postsInfo)
	assert.NoError(t, err)
	expectedPostsJSON, err := json.Marshal(postsInfoExpected)
	assert.JSONEq(t, string(expectedPostsJSON), string(postsJSON))
}
