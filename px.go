package px

import (
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/iancoleman/strcase"
	"github.com/stevenzack/tools/strToolkit"
)

var (
	pluralizeClient = pluralize.NewClient()
)

func ToTableName(s string) string {
	s = strcase.ToSnake(s)
	s = pluralizeClient.Plural(s)
	return s
}
func convertIndexToFieldName(tablename, s string) string {
	s = strings.TrimPrefix(s, tablename)
	s = strings.TrimSuffix(s, "_idx")
	s = strings.TrimPrefix(s, "_")
	s = strings.TrimSuffix(s, "_")
	return s
}
func toWhere(where string) string {
	where = strToolkit.TrimStart(where, " ")
	if where != "" && !strings.HasPrefix(where, "where") {
		if strings.HasPrefix(where, "order") || strings.HasPrefix(where, "limit") {
			return " " + where
		}
		where = " where " + where
	}
	return where
}
