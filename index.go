package px

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
)

type (
	indexModel struct {
		isPrimaryKey bool
		unique       bool
		keys         []indexKey
	}
	indexKey struct {
		lower    bool
		key      string
		sequence string
	}
	IndexSchema struct {
		SchemaName string `db:"schemaname"`
		TableName  string `db:"tablename"`
		IndexName  string `db:"indexname"`
		IndexDef   string `db:"indexdef"`
	}
)

type sortByIndexKey []indexKey

func (a sortByIndexKey) Len() int           { return len(a) }
func (a sortByIndexKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByIndexKey) Less(i, j int) bool { return a[i].key < a[j].key }

func (i *indexModel) ToIndexName(tableName string) string {
	buf := new(strings.Builder)
	buf.WriteString(tableName + "_")
	for _, k := range i.keys {
		if k.lower {
			buf.WriteString("lower_")
			continue
		}
		buf.WriteString(k.key + "_")
	}
	buf.WriteString("idx")
	return buf.String()
}
func (i *indexModel) JoinKeys(sep string) string {
	builder := new(strings.Builder)
	for index, v := range i.keys {
		builder.WriteString(v.key)
		if index < len(i.keys)-1 {
			builder.WriteString(sep)
		}
	}
	return builder.String()
}
func toIndexModels(indexes map[string]string) (*indexModel, []indexModel, error) {
	imodels := []indexModel{}
	groupMap := make(map[string]indexModel)
	for key, index := range indexes {
		vs, e := url.ParseQuery(strings.ReplaceAll(index, ",", "&"))
		if e != nil {
			return nil, nil, errors.New("field '" + key + "', invalid index tag format:" + index)
		}

		imodel := indexModel{}
		lower := false
		group := ""
		for k := range vs {
			v := vs.Get(k)
			switch k {
			case "single":
				if len(imodel.keys) > 0 {
					return nil, nil, errors.New("field '" + key + "': duplicated key 'single'")
				}
				indexKey := indexKey{
					key:      key,
					sequence: "asc",
				}
				if v == "desc" {
					indexKey.sequence = "desc"
				}
				imodel.keys = append(imodel.keys, indexKey)
			case "unique", "uniq":
				imodel.unique = true
			case "group":
				group = vs.Get(k)
			case "lower":
				lower = v == "true"
			default:
				return nil, nil, errors.New("field '" + key + "', unsupported key:" + k)
			}
		}

		// normal index
		if group == "" {
			if len(imodel.keys) == 0 {
				imodel.keys = append(imodel.keys, indexKey{
					key:      key,
					sequence: "asc",
					lower:    lower,
				})
			}
			imodels = append(imodels, imodel)
			continue
		}

		//another single index
		if len(imodel.keys) > 0 {
			imodel.keys[0].lower = lower
			imodels = append(imodels, imodel)
		}

		//group index
		before, ok := groupMap[group]
		if !ok {
			if strings.HasPrefix(group, "pkey") {
				before.isPrimaryKey = true
				before.keys = append(before.keys, indexKey{
					key: "id",
				})
			}
			before.keys = append(before.keys, indexKey{
				key:      strToolkit.SubBefore(key, ",", key),
				sequence: "asc",
				lower:    lower,
			})
			if strings.HasPrefix(group, "unique") {
				before.unique = true
			}

			groupMap[group] = before
			continue
		}

		//append
		before.keys = append(before.keys, indexKey{
			key:      key,
			sequence: "asc",
			lower:    lower,
		})
		groupMap[group] = before
	}

	//add group indexes
	var primaryKeyModel *indexModel
	for _, v := range groupMap {
		if v.isPrimaryKey {
			primaryKeyModel = &v
			continue
		}
		sort.Sort(sortByIndexKey(v.keys))
		imodels = append(imodels, v)
	}
	return primaryKeyModel, imodels, nil
}

// createIndexFromField create index with format like: map[column_name]"single=asc,unique=true,lower=true,group=unique"
func (b *BaseModel[T]) createIndexFromField(imodels []indexModel) error {
	if len(imodels) == 0 {
		return nil
	}

	for _, imodel := range imodels {
		e := b.createIndex(imodel)
		if e != nil {
			log.Println(e)
			return e
		}
	}
	return nil
}

func (b *BaseModel[T]) createIndex(imodel indexModel) error {
	builder := new(strings.Builder)
	builder.WriteString("create ")
	if imodel.unique {
		builder.WriteString("unique ")
	}
	builder.WriteString("index on " + b.Schema + "." + b.TableName + " (")
	for i, key := range imodel.keys {
		if key.lower {
			builder.WriteString("lower(" + key.key + ")")
		} else {
			builder.WriteString(key.key)
		}
		builder.WriteString(" asc")
		if i < len(imodel.keys)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(")")
	query := builder.String()
	_, e := b.Pool.Exec(context.Background(), query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel[T]) dropIndex(name string) error {
	query := `drop index ` + name
	_, e := b.Pool.Exec(context.Background(), query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel[T]) GetIndexes() ([]IndexSchema, error) {
	rows, e := b.Pool.Query(context.Background(), `select schemaname,tablename,indexname,indexdef from pg_indexes where tablename=$1`, b.TableName)
	if e != nil {
		return nil, e
	}

	vs := []IndexSchema{}
	for rows.Next() {
		v := IndexSchema{}
		e = rows.Scan(&v.SchemaName, &v.TableName, &v.IndexName, &v.IndexDef)
		if e != nil {
			break
		}
		vs = append(vs, v)
	}
	//check err
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs, nil
}
