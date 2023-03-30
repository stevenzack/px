package px

import (
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	"github.com/iancoleman/strcase"
)

func ToTableName(s string) string {
	s = strcase.ToSnake(s)
	switch s {
	case "user", "order":
		return s + "s"
	}
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
