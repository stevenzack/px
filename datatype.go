package px

import (
	"errors"
	"reflect"
	"strconv"

	"github.com/StevenZack/tools/strToolkit"
)

func ToPostgreType(t reflect.Type, dbTag string, length, limit int) (string, error) {
	isId := dbTag == "id"
	switch t.Kind() {
	case reflect.Int, reflect.Int64:
		return "bigint not null default 0", nil
	case reflect.Int32:
		return "integer not null default 0", nil
	case reflect.Int16:
		return "smallint not null default 0", nil
	case reflect.Uint, reflect.Uint64:
		if isId {
			return "bigserial not null", nil
		}
		return "bigint not null default 0 check ( " + dbTag + ">-1 )", nil
	case reflect.Uint32:
		if isId {
			return "serial not null", nil
		}
		return "integer not null default 0 check ( " + dbTag + ">-1 )", nil
	case reflect.Uint16:
		if isId {
			return "smallserial not null", nil
		}
		return "smallint not null default 0 check ( " + dbTag + ">-1 )", nil
	case reflect.Float64:
		return "double precision not null default 0", nil
	case reflect.String:
		if limit > 0 {
			return "varchar(" + strconv.Itoa(limit) + ") not null default ''", nil
		}
		if length > 0 {
			return "char(" + strconv.Itoa(length) + ") not null", nil
		}
		return "text not null default ''", nil
	case reflect.Bool:
		return "boolean not null default false", nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8:
			return "bytea", nil
		case reflect.Int32:
			return "integer[]", nil
		case reflect.Int, reflect.Int64:
			return "bigint[]", nil
		case reflect.String:
			return "text[]", nil
		}
	case reflect.Struct:
		switch t.String() {
		case "time.Time":
			return "timestamp with time zone not null default '0001-01-01 00:00:00'", nil
			// case "sql.NullString":
			// 	if limit > 0 {
			// 		return "varchar(" + strconv.Itoa(limit) + ")", nil
			// 	}
			// 	return "text", nil
			// case "sql.NullBool":
			// 	return "boolean", nil
			// case "sql.NullInt32":
			// 	return "integer", nil
			// case "sql.NullInt64":
			// 	return "bigint", nil
			// case "sql.NullFloat64":
			// 	return "double precision", nil
			// case "sql.NullTime":
			// 	return "timestamp with time zone", nil
			// case "pq.Int64Array":
			// 	return "bigint[]", nil
			// case "pq.Int32Array":
			// 	return "integer[]", nil
			// case "pq.StringArray":
			// 	return "text[]", nil
			// case "pq.BoolArray":
			// 	return "boolean[]", nil
		}
	case reflect.Map:
		return "jsonb", nil
	case reflect.Ptr: // Pointer type
		t = t.Elem()
		switch t.Kind() {
		case reflect.Int, reflect.Int64:
			return "bigint", nil
		case reflect.Int32:
			return "integer", nil
		case reflect.Int16:
			return "smallint", nil
		case reflect.Uint, reflect.Uint64:
			return "bigint check ( " + dbTag + ">-1 or " + dbTag + " = null)", nil
		case reflect.Uint32:
			return "integer check ( " + dbTag + ">-1 or " + dbTag + " = null)", nil
		case reflect.Uint16:
			if isId {
				return "smallserial", nil
			}
			return "smallint check ( " + dbTag + ">-1 or " + dbTag + " = null)", nil
		case reflect.Float64:
			return "double precision", nil
		case reflect.String:
			if limit > 0 {
				return "varchar(" + strconv.Itoa(limit) + ")", nil
			}
			if length > 0 {
				return "char(" + strconv.Itoa(length) + ")", nil
			}
			return "text", nil
		case reflect.Bool:
			return "boolean", nil
		case reflect.Struct:
			switch t.String() {
			case "time.Time":
				return "timestamp with time zone", nil
			}
		}
	}
	return "", errors.New("unsupport field type:" + t.String() + ",kind=" + t.Kind().String())
}

func toPgPrimitiveType(dbType string) string {
	dbType = strToolkit.SubBefore(dbType, " ", dbType)
	dbType = strToolkit.SubBefore(dbType, "(", dbType)
	switch dbType {
	case "serial":
		dbType = "integer"
	case "smallserial":
		dbType = "smallint"
	case "bigserial":
		dbType = "bigint"
	case "char", "varchar":
		dbType = "character"
	}

	return dbType
}

func String(s string) *string {
	return &s
}

func Bool(b bool) *bool {
	return &b
}
func Int(i int) *int {
	return &i
}
func Uint(i uint) *uint {
	return &i
}
func Float(f float64) *float64 {
	return &f
}
func Int64(i int64) *int64 {
	return &i
}
func Uint64(i uint64) *uint64 {
	return &i
}
func Int32(i int32)*int32{
	return &i
}
func Uint32(i uint32)*uint32{
	return &i
}
