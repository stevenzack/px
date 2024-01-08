package px

import (
	"errors"
	"strings"

	"github.com/stevenzack/tools/strToolkit"
)

func ParseDsn(s string) (map[string]string, error) {
	ss := strings.Split(s, " ")
	out := make(map[string]string)
	for _, s := range ss {
		kv := strings.Split(s, "=")
		if len(kv) != 2 {
			return nil, errors.New("Invalid dsn at:" + s)
		}
		out[kv[0]] = strToolkit.TrimBoth(kv[1], "\"")
	}
	return out, nil
}

func FormatDsn(m map[string]string) string {
	buf := new(strings.Builder)
	for k, v := range m {
		buf.WriteString(k + "=" + v + " ")
	}
	return buf.String()
}
