package postgres

import (
	"reflect"
	"sync"

	"github.com/Cobiro/go-utils/tagstring"
	"github.com/Cobiro/go-utils/text"
)

type StructInfo struct {
	Name         string               // Struct name
	DBName       string               // Extracted or snake_cased db name
	Fields       map[string]FieldInfo // Info about each field
	Type         reflect.Type
	NonRelFields int // Count of how many fields are non-relational
	// IsSubStruct bool // Does another
}

type FieldInfo struct {
	Index    int                 // Field index in struct
	Name     string              // Field name
	DBName   string              // Extracted or snake_cased db name
	Tag      tagstring.TagString // Struct field tag
	IsParent bool                // Is this a struct field?
	IsSlice  bool                // Is this a slice field? E.g. multi relation
	RelType  string              // "", "one", "many"
	Type     reflect.Type
	BaseType reflect.Type // Indirected down to the basic type
}

var structCache = map[reflect.Type]StructInfo{}
var structCacheLock = sync.Mutex{}

func getStructInfo(strct interface{}) StructInfo {
	// Get the actual type of the struct
	// Assuming that we know a little about how this can look (ptr to slice)
	structType := getBaseType(strct)

	// Check if we have this type cached already
	structCacheLock.Lock()
	if structInfo, ok := structCache[structType]; ok {
		structCacheLock.Unlock()
		return structInfo
	}
	structCacheLock.Unlock()

	// Start building the StructInfo
	structInfo := StructInfo{
		Name:         structType.Name(),
		DBName:       text.SnakeCase(structType.Name()),
		Type:         structType,
		Fields:       map[string]FieldInfo{},
		NonRelFields: 0,
	}

	// Store sub structs for later extraction
	subStructs := []reflect.Type{}

	// Extract fields info
	numFields := structType.NumField()
	for i := 0; i < numFields; i++ {
		field := structType.Field(i)
		fieldInfo := getFieldInfo(field)
		if fieldInfo == nil {
			continue
		}
		fieldInfo.Index = i

		// If this field has a sub struct, save it for later
		if fieldInfo.IsParent {
			subStructs = append(subStructs, fieldInfo.BaseType)
		} else {
			structInfo.NonRelFields += 1
		}

		// Add the field to the struct
		structInfo.Fields[fieldInfo.DBName] = *fieldInfo

	}

	// Check if we have this type cached already
	structCacheLock.Lock()
	structCache[structType] = structInfo
	structCacheLock.Unlock()

	// Extract any sub structs
	for _, subStruct := range subStructs {
		getStructInfo(reflect.New(subStruct).Interface())
	}

	return structInfo
}

func getFieldInfo(field reflect.StructField) (fieldInfo *FieldInfo) {
	fieldInfo = &FieldInfo{
		Name:     field.Name,
		DBName:   "",
		Type:     field.Type,
		BaseType: field.Type, // Indirected down to the basic type
		Tag:      tagstring.TagString(field.Tag),
	}

	//
	// Extract info from tags
	//

	if fieldInfo.Tag.Get("reltype") != "" && fieldInfo.Tag.Get("ftable") != "" {
		fieldInfo.RelType = fieldInfo.Tag.Get("reltype")
		fieldInfo.DBName = fieldInfo.Tag.Get("ftable")
	} else if fieldInfo.Tag.Get("col") != "" {
		fieldInfo.DBName = fieldInfo.Tag.Get("col")
	} else if fieldInfo.Tag.Get("db") != "" {
		fieldInfo.DBName = fieldInfo.Tag.Get("db")
	} else {
		// Don't deal with unmarked structs... To freakin' difficult
		return nil
	}

	//
	// Extract type info. Whether it's a struct or slice of structs
	//

	// Dereference
	if fieldInfo.BaseType.Kind() == reflect.Ptr {
		fieldInfo.BaseType = fieldInfo.BaseType.Elem()
	}

	// Check if this is a slice
	if fieldInfo.BaseType.Kind() == reflect.Slice {
		fieldInfo.BaseType = fieldInfo.BaseType.Elem()
		fieldInfo.IsSlice = true
	}

	// Again... In case of e.g.: []*Struct
	if fieldInfo.BaseType.Kind() == reflect.Ptr {
		fieldInfo.BaseType = fieldInfo.BaseType.Elem()
	}

	if fieldInfo.BaseType.Kind() == reflect.Struct {
		// Rule out some wellknown types, that could be vcolumn vals
		// Not that important.. They will just be cached...
		switch fieldInfo.BaseType.String() {
		case "time.Time", "json.RawMessage":
			// Do nothing. These are legit single val structs

		case "sql.RawBytes", "sql.NullString", "sql.NullInt64", "sql.NullFloat64", "sql.NullBool":
			// Do nothing. These are legit single val structs
		default:
			fieldInfo.IsParent = true
		}
	}

	return
}

// getBaseType indirects until the type is no longer a slice or pointer
func getBaseType(i interface{}) (baseType reflect.Type) {
	baseType = reflect.TypeOf(i)

	for baseType.Kind() == reflect.Ptr || baseType.Kind() == reflect.Slice {
		baseType = baseType.Elem()
	}

	return
}
