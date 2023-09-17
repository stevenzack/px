package px

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BaseModel[T any] struct {
	Type      reflect.Type
	Dsn       string
	Pool      *pgxpool.Pool
	Database  string
	Schema    string
	TableName string

	dbTags  []string
	pgTypes []string
}

const (
	driverName = "postgres"
)

var (
	AutoSyncTableSchema  = false
	AutoDropRemoteColumn = false
)

func MustNewBaseModel[T any](dsn string) *BaseModel[T] {
	v, e := NewBaseModel[T](dsn)
	if e != nil {
		log.Fatal(e)
	}
	return v
}
func NewBaseModel[T any](dsn string) (*BaseModel[T], error) {
	model, _, e := NewBaseModelWithCreated[T](dsn)
	return model, e
}

func NewBaseModelWithCreated[T any](dsn string) (*BaseModel[T], bool, error) {
	dsn = strings.ReplaceAll(dsn, "postgresql://", "postgres://")
	created := false
	var data T
	t := reflect.TypeOf(data)

	model := &BaseModel[T]{
		Dsn:       dsn,
		Type:      t,
		Database:  strToolkit.SubAfterLast(dsn, "/", ""),
		Schema:    "public",
		TableName: ToTableName(t.Name()),
	}

	//validate
	if model.Database == "" {
		return nil, false, errors.New("invalid dsn ")
	}

	//pool
	var e error
	model.Pool, e = pgxpool.New(context.Background(), dsn)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, false, errors.New("data must be struct type")
	}

	indexes := make(map[string]string)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if i == 0 {
			switch field.Type.Kind() {
			case reflect.Uint,
				reflect.Uint64,
				reflect.Uint32,
				reflect.Uint16,
				reflect.String:
			default:
				return nil, false, errors.New("The first field " + field.Name + "'s type must be one of uint,uint32,uint64,uint16,string")
			}
		}

		//dbTag
		dbTag := strcase.ToSnake(field.Name)
		if i == 0 && dbTag != "id" {
			return nil, false, errors.New("The first field's name must be Id or ID")
		}

		//index
		if index, ok := field.Tag.Lookup("index"); ok {
			indexes[dbTag] = index
		}

		//limit
		limit := 0
		if limitStr, ok := field.Tag.Lookup("limit"); ok {
			limit, e = strconv.Atoi(limitStr)
			if e != nil {
				log.Println(e)
				return nil, false, errors.New("Invalid limit tag format:" + limitStr + " for field " + field.Name)
			}
		}
		length := 0
		if lengthStr, ok := field.Tag.Lookup("length"); ok {
			length, e = strconv.Atoi(lengthStr)
			if e != nil {
				log.Println(e)
				return nil, false, errors.New("Invalid length tag format:" + lengthStr + " for field " + field.Name)
			}
		}

		//pgType
		pgType, e := ToPostgreType(field.Type, dbTag, length, limit)
		if e != nil {
			log.Println(e)
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
		}

		model.dbTags = append(model.dbTags, dbTag)
		model.pgTypes = append(model.pgTypes, pgType)
	}
	primaryKeyModel, localIndexList, e := toIndexModels(indexes)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	if AutoSyncTableSchema {
		//desc
		remoteColumnList, e := DescTable(model.Pool, model.Database, model.Schema, model.TableName)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}

		//create table
		if len(remoteColumnList) == 0 {
			e = model.createTable(primaryKeyModel)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			//create index
			e = model.createIndexFromField(localIndexList)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			return model, true, nil
		}

		// columns check
		remoteColumns := make(map[string]Column)
		for _, c := range remoteColumnList {
			remoteColumns[c.ColumnName] = c
		}

		// local columns to be created
		localColumns := make(map[string]string)
		for i, db := range model.dbTags {
			localColumns[db] = model.pgTypes[i]

			remote, ok := remoteColumns[db]
			if !ok {
				//auto-create field on remote database
				log.Println("Remote column '" + db + "' to be created")
				e = model.addColumn(db, model.pgTypes[i])
				if e != nil {
					log.Println(e)
					return nil, false, e
				}
				continue
			}

			//type check
			dbType := toPgPrimitiveType(model.pgTypes[i])
			remoteType := strToolkit.SubBefore(remote.DataType, " ", remote.DataType)
			if strings.HasSuffix(dbType, "[]") {
				dbType = "ARRAY"
			}
			if dbType != remoteType {
				return nil, false, errors.New("Found local field " + db + "'s type '" + dbType + "' doesn't match remote column type:" + remoteType)
			}
			if strings.Contains(model.pgTypes[i], "not null") != (remote.IsNullable == "NO") {
				return nil, false, errors.New("Found local field " + db + "'s nullability '" + model.pgTypes[i] + "' doesn't match remote column nullability :" + remote.IsNullable)
			}
		}

		//remote columns to be dropped
		for _, remote := range remoteColumnList {
			_, ok := localColumns[remote.ColumnName]
			if !ok {
				if AutoDropRemoteColumn {
					//auto-drop remote column
					log.Println("Remote column '" + remote.ColumnName + "' to be dropped")
					e = model.dropColumn(remote.ColumnName)
					if e != nil {
						log.Println(e)
						return nil, false, e
					}
					continue
				}
				return nil, false, errors.New("Remote column '" + remote.ColumnName + "' to be dropped")
			}
		}

		// index check
		remoteIndexList, e := model.GetIndexes()
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		remoteIndexes := make(map[string]IndexSchema)
		for _, remote := range remoteIndexList {
			remoteIndexes[remote.IndexName] = remote
		}

		// indexes to be created
		localIndexes := make(map[string]indexModel)
		for _, local := range localIndexList {
			localIndexes[local.ToIndexName(model.TableName)] = local
			remote, ok := remoteIndexes[local.ToIndexName(model.TableName)]
			if !ok {
				//auto-create index on remote database
				log.Println("Remote index '" + local.ToIndexName(model.TableName) + "' to be created")
				e = model.createIndex(local)
				if e != nil {
					log.Println(e)
					return nil, false, e
				}
				continue
			}

			//unique check
			if local.unique != strings.Contains(remote.IndexDef, "UNIQUE") {
				return nil, false, errors.New("Index '" + local.ToIndexName(model.TableName) + "' unique option is inconsistant with remote database: " + strconv.FormatBool(local.unique) + " vs " + strconv.FormatBool(strings.Contains(remote.IndexDef, "UNIQUE")))
			}
		}

		//indexes to be dropped
		for _, remote := range remoteIndexList {
			log.Println(remote.IndexName)
			if strings.Contains(remote.IndexName, "_pkey") {
				continue
			}

			_, ok := localIndexes[remote.IndexName]
			if !ok {
				if !strToolkit.SliceContains(model.dbTags, convertIndexToFieldName(model.TableName, remote.IndexName)) {
					continue
				}
				log.Println("Remote index '" + remote.IndexName + "' to be dropped")
				e = model.dropIndex(remote.IndexName)
				if e != nil {
					log.Println(e)
					return nil, false, e
				}
				continue
			}
		}
	}

	return model, created, nil
}

func (b *BaseModel[T]) createTable(primaryKeyModel *indexModel) error {
	query := b.GetCreateTableSQL(primaryKeyModel)
	_, e := b.Pool.Exec(context.Background(), query)
	if e != nil {
		return fmt.Errorf("%w: %s", e, query)
	}
	return nil
}

func (b *BaseModel[T]) addColumn(name, typ string) error {
	_, e := b.Pool.Exec(context.Background(), `alter table `+b.Schema+`.`+b.TableName+` add column `+name+` `+typ)
	if e != nil {
		log.Println(e)
		return e
	}
	return e
}

func (b *BaseModel[T]) dropColumn(name string) error {
	_, e := b.Pool.Exec(context.Background(), `alter table `+b.Schema+`.`+b.TableName+` drop column `+name)
	if e != nil {
		log.Println(e)
		return e
	}
	return nil
}

func (b *BaseModel[T]) GetCreateTableSQL(primaryKeyModel *indexModel) string {
	builder := new(strings.Builder)
	builder.WriteString(`create table ` + b.Schema + `.` + b.TableName + ` (`)
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag + " ")
		builder.WriteString(b.pgTypes[i])
		if i == 0 && primaryKeyModel == nil {
			builder.WriteString(" primary key")
		}
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	if primaryKeyModel != nil {
		builder.WriteString(", primary key (" + primaryKeyModel.JoinKeys(",") + ")")
	}
	builder.WriteString(`)`)
	log.Println(builder.String())
	return builder.String()
}

// GetInsertSQL returns insert SQL without returning id
func (b *BaseModel[T]) GetInsertSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`insert into ` + b.Schema + `.` + b.TableName + ` (`)

	values := new(strings.Builder)
	values.WriteString("values (")

	argsIndex := []int{}

	for i, dbTag := range b.dbTags {
		dbType := b.pgTypes[i]
		if strings.Contains(dbType, "serial") {
			continue
		}

		argsIndex = append(argsIndex, i)

		builder.WriteString(dbTag)
		values.WriteString("$" + strconv.Itoa(len(argsIndex)))

		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
			values.WriteString(",")
		}

	}

	builder.WriteString(")")
	values.WriteString(")")

	builder.WriteString(values.String())

	return argsIndex, builder.String()
}

// GetInsertReturningSQL returns insert SQL with returning id
func (b *BaseModel[T]) GetInsertReturningSQL() ([]int, string) {
	argsIndex, query := b.GetInsertSQL()
	return argsIndex, query + " returning " + b.dbTags[0]
}

// GetSelectSQL returns fieldIndexes, and select SQL
func (b *BaseModel[T]) GetSelectSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`select `)
	fieldIndexes := []int{}
	for i, dbTag := range b.dbTags {
		builder.WriteString(b.TableName + "." + dbTag)
		fieldIndexes = append(fieldIndexes, i)
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(" from " + b.TableName)
	return fieldIndexes, builder.String()
}

// id,name,create_at
func (b *BaseModel[T]) GetSelectFields() ([]int, string) {
	builder := new(strings.Builder)
	fieldIndexes := []int{}
	for i, dbTag := range b.dbTags {
		builder.WriteString(b.TableName + "." + dbTag)
		fieldIndexes = append(fieldIndexes, i)
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	return fieldIndexes, builder.String()
}

// Insert inserts v (*struct or struct type)
func (b *BaseModel[T]) Insert(v T) (any, error) {
	//validate
	value := reflect.ValueOf(v)
	t := value.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		value = value.Elem()
	}
	if t.String() != b.Type.String() {
		return nil, errors.New("Wrong insert type:" + t.String() + " for table " + b.TableName)
	}

	//args
	argsIndex, query := b.GetInsertReturningSQL()
	args := []any{}
	for _, i := range argsIndex {
		field := value.Field(i)
		args = append(args, field.Interface())
	}

	//exec
	id := reflect.New(b.Type.Field(0).Type)
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(id.Interface())
	if e != nil {
		return nil, e
	}

	return id.Elem().Interface(), nil
}

// Find finds a document (*struct type) by id
func (b *BaseModel[T]) Find(id any) (*T, error) {
	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	fieldArgs := []any{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}

	query = query + ` where ` + b.dbTags[0] + `=$1`
	e := b.Pool.QueryRow(context.Background(), query, id).Scan(fieldArgs...)
	if e != nil {
		if strings.Contains(e.Error(), "no rows") {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface().(*T), nil
}

// FindWhere finds a document (*struct type) that matches 'where' condition
func (b *BaseModel[T]) FindWhere(where string, args ...any) (*T, error) {
	//where
	where = toWhere(where)

	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	query = query + where
	fieldArgs := []any{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(fieldArgs...)
	if e != nil {
		if strings.Contains(e.Error(), "no rows") {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface().(*T), nil
}

// QueryWhere queries documents ([]struct type) that matches 'where' condition
func (b *BaseModel[T]) QueryWhere(where string, args ...any) ([]T, error) {
	where = toWhere(where)

	fieldIndexes, query := b.GetSelectSQL()

	//query
	query = query + where
	rows, e := b.Pool.Query(context.Background(), query, args...)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}

	vs := reflect.MakeSlice(reflect.SliceOf(b.Type), 0, 2)
	for rows.Next() {
		v := reflect.New(b.Type)
		fieldArgs := []any{}
		for _, i := range fieldIndexes {
			fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
		}
		e = rows.Scan(fieldArgs...)
		if e != nil {
			break
		}
		vs = reflect.Append(vs, v.Elem())
	}

	// check err
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs.Interface().([]T), nil
}

// QueryWhere queries documents ([]struct type) that matches 'where' condition
func (b *BaseModel[T]) Query(queryTrail string, args ...any) ([]T, error) {
	fieldIndexes, query := b.GetSelectSQL()

	//query
	query = query + " " + queryTrail
	rows, e := b.Pool.Query(context.Background(), query, args...)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}

	vs := reflect.MakeSlice(reflect.SliceOf(b.Type), 0, 2)
	for rows.Next() {
		v := reflect.New(b.Type)
		fieldArgs := []any{}
		for _, i := range fieldIndexes {
			fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
		}
		e = rows.Scan(fieldArgs...)
		if e != nil {
			break
		}
		vs = reflect.Append(vs, v.Elem())
	}

	// check err
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs.Interface().([]T), nil
}

func (b *BaseModel[T]) Exists(id any) (bool, error) {
	//scan
	num := 0
	query := `select 1 from ` + b.TableName + ` where ` + b.dbTags[0] + `=$1 limit 1`
	e := b.Pool.QueryRow(context.Background(), query, id).Scan(&num)
	if e != nil {
		if strings.Contains(e.Error(), "no rows") {
			return false, sql.ErrNoRows
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel[T]) ExistsWhere(where string, args ...any) (bool, error) {
	//where
	where = toWhere(where)

	//scan
	num := 0
	query := `select 1 from ` + b.TableName + where + ` limit 1`
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(&num)
	if e != nil {
		if strings.Contains(e.Error(), "no rows") {
			return false, sql.ErrNoRows
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel[T]) CountWhere(where string, args ...any) (int64, error) {
	where = toWhere(where)

	//scan
	var num int64
	query := `select count(*) as count from ` + b.TableName + where
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(&num)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return num, nil
}

func (b *BaseModel[T]) UpdateSet(where, sets string, args ...any) (int64, error) {
	where = toWhere(where)
	query := `update ` + b.TableName + ` set ` + sets + where
	result, e := b.Pool.Exec(context.Background(), query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected(), nil
}

func (b *BaseModel[T]) Clear() error {
	query := `truncate table ` + b.TableName
	_, e := b.Pool.Exec(context.Background(), query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel[T]) Truncate() error {
	return b.Clear()
}

func (b *BaseModel[T]) Delete(id any) (int64, error) {
	query := `delete from ` + b.TableName + ` where ` + b.dbTags[0] + `=$1`
	result, e := b.Pool.Exec(context.Background(), query, id)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected(), nil
}

func (b *BaseModel[T]) DeleteWhere(where string, args ...any) (int64, error) {
	where = toWhere(where)

	query := `delete from ` + b.TableName + where
	result, e := b.Pool.Exec(context.Background(), query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected(), nil
}

func (b *BaseModel[T]) FindAndUpdateSet(where, sets string, args ...any) (*T, error) {
	where = toWhere(where)
	query := `update ` + b.TableName + ` set ` + sets + where
	//scan
	v := reflect.New(b.Type)
	fieldIndexes, selection := b.GetSelectFields()
	fieldArgs := []any{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}

	query = query + ` returning ` + selection
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(fieldArgs...)
	if e != nil {
		if strings.Contains(e.Error(), "no rows") {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface().(*T), nil
}

func (b *BaseModel[T]) QueryAndUpdateSet(where, sets string, args ...any) ([]T, error) {
	where = toWhere(where)
	query := `update ` + b.TableName + ` set ` + sets + where
	//scan
	fieldIndexes, selection := b.GetSelectFields()

	query = query + ` returning ` + selection
	rows, e := b.Pool.Query(context.Background(), query, args...)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	vs := reflect.MakeSlice(reflect.SliceOf(b.Type), 0, 2)
	for rows.Next() {
		v := reflect.New(b.Type)
		fieldArgs := []any{}
		for _, i := range fieldIndexes {
			fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
		}
		e = rows.Scan(fieldArgs...)
		if e != nil {
			break
		}
		vs = reflect.Append(vs, v.Elem())
	}

	// check err
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs.Interface().([]T), nil
}
