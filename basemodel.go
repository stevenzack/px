package px

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/StevenZack/tools/strToolkit"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BaseModel struct {
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
	AutoDropColumn = false
)

func NewBaseModel(dsn string, data interface{}) (*BaseModel, error) {
	model, _, e := NewBaseModelWithCreated(dsn, data)
	return model, e
}

func NewBaseModelWithCreated(dsn string, data interface{}) (*BaseModel, bool, error) {
	created := false
	t := reflect.TypeOf(data)
	dsnURL, e := url.Parse(dsn)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	model := &BaseModel{
		Dsn:       dsn,
		Type:      t,
		Database:  strings.TrimLeft(dsnURL.Path, "/"),
		Schema:    "public",
		TableName: ToTableName(t.Name()),
	}

	//validate
	if model.Database == "" {
		return nil, false, errors.New("dsn: dbname is not set")
	}

	//pool
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
		dbTag, ok := field.Tag.Lookup("db")
		if !ok {
			return nil, false, errors.New("field " + field.Name + " has no `db` tag specified")
		}
		if i == 0 && dbTag != "id" {
			return nil, false, errors.New("The first field's `db` tag must be id")
		}
		if dbTag != strcase.ToSnake(dbTag) {
			return nil, false, errors.New("Field '" + field.Name + "'s `db` tag is not in snake case")
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
	localIndexList, e := toIndexModels(indexes)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	//desc
	remoteColumnList, e := DescTable(model.Pool, model.Database, model.Schema, model.TableName)
	if e != nil {
		if !strings.Contains(e.Error(), fmt.Sprintf(`database "%s" does not exist`, model.Database)) {
			log.Println(e)
			return nil, false, e
		}
		// database not exists
		dsnURL.Path = "/postgres"
		pool, e := pgx.Connect(context.Background(), dsnURL.String())
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		s := "create database " + model.Database
		_, e = pool.Exec(context.Background(), s)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		e = pool.Close(context.Background())
		if e != nil {
			log.Println(e)
			return nil, false, e
		}

		model.Pool, e = pgxpool.New(context.Background(), dsn)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
	}

	//create table
	if len(remoteColumnList) == 0 {
		e = model.createTable()
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
			if AutoDropColumn {
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
		if strings.Contains(remote.IndexName, "_pkey") {
			continue
		}
		_, ok := localIndexes[remote.IndexName]
		if !ok {
			log.Println("Remote index '" + remote.IndexName + "' to be dropped")
			e = model.dropIndex(remote.IndexName)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			continue
		}
	}

	return model, created, nil
}

func (b *BaseModel) createTable() error {
	query := b.GetCreateTableSQL()
	_, e := b.Pool.Exec(context.Background(), query)
	if e != nil {
		return fmt.Errorf("%w: %s", e, query)
	}
	return nil
}

func (b *BaseModel) addColumn(name, typ string) error {
	_, e := b.Pool.Exec(context.Background(), `alter table `+b.Schema+`.`+b.TableName+` add column `+name+` `+typ)
	if e != nil {
		log.Println(e)
		return e
	}
	return e
}

func (b *BaseModel) dropColumn(name string) error {
	_, e := b.Pool.Exec(context.Background(), `alter table `+b.Schema+`.`+b.TableName+` drop column `+name)
	if e != nil {
		log.Println(e)
		return e
	}
	return nil
}

func (b *BaseModel) GetCreateTableSQL() string {
	builder := new(strings.Builder)
	builder.WriteString(`create table ` + b.Schema + `.` + b.TableName + ` (`)
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag + " ")
		builder.WriteString(b.pgTypes[i])
		if i == 0 {
			builder.WriteString(" primary key")
		}
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(`)`)
	return builder.String()
}

// GetInsertSQL returns insert SQL without returning id
func (b *BaseModel) GetInsertSQL() ([]int, string) {
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
func (b *BaseModel) GetInsertReturningSQL() ([]int, string) {
	argsIndex, query := b.GetInsertSQL()
	return argsIndex, query + " returning " + b.dbTags[0]
}

// GetSelectSQL returns fieldIndexes, and select SQL
func (b *BaseModel) GetSelectSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString(`select `)
	fieldIndexes := []int{}
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag)
		fieldIndexes = append(fieldIndexes, i)
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(" from " + b.TableName)
	return fieldIndexes, builder.String()
}

// Insert inserts v (*struct or struct type)
func (b *BaseModel) Insert(v interface{}) (interface{}, error) {
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
	args := []interface{}{}
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
func (b *BaseModel) Find(id interface{}) (interface{}, error) {
	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	fieldArgs := []interface{}{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}

	query = query + ` where ` + b.dbTags[0] + `=$1`
	e := b.Pool.QueryRow(context.Background(), query, id).Scan(fieldArgs...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, e
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface(), nil
}

// FindWhere finds a document (*struct type) that matches 'where' condition
func (b *BaseModel) FindWhere(where string, args ...interface{}) (interface{}, error) {
	//where
	where = toWhere(where)

	//scan
	v := reflect.New(b.Type)
	fieldIndexes, query := b.GetSelectSQL()
	query = query + where
	fieldArgs := []interface{}{}
	for _, i := range fieldIndexes {
		fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
	}
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(fieldArgs...)
	if e != nil {
		if e == sql.ErrNoRows {
			return nil, e
		}
		return nil, fmt.Errorf("%w:%s", e, query)
	}
	return v.Interface(), nil
}

// QueryWhere queries documents ([]*struct type) that matches 'where' condition
func (b *BaseModel) QueryWhere(where string, args ...interface{}) (interface{}, error) {
	where = toWhere(where)

	fieldIndexes, query := b.GetSelectSQL()

	//query
	query = query + where
	rows, e := b.Pool.Query(context.Background(), query, args...)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}

	vs := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(b.Type)), 0, 2)
	for rows.Next() {
		v := reflect.New(b.Type)
		fieldArgs := []interface{}{}
		for _, i := range fieldIndexes {
			fieldArgs = append(fieldArgs, v.Elem().Field(i).Addr().Interface())
		}
		e = rows.Scan(fieldArgs...)
		if e != nil {
			break
		}
		vs = reflect.Append(vs, v)
	}

	// check err
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return vs.Interface(), nil
}

func (b *BaseModel) Exists(id interface{}) (bool, error) {
	//scan
	num := 0
	query := `select 1 from ` + b.TableName + ` where ` + b.dbTags[0] + `=$1 limit 1`
	e := b.Pool.QueryRow(context.Background(), query, id).Scan(&num)
	if e != nil {
		if e == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel) ExistsWhere(where string, args ...interface{}) (bool, error) {
	//where
	where = toWhere(where)

	//scan
	num := 0
	query := `select 1 from ` + b.TableName + where + ` limit 1`
	e := b.Pool.QueryRow(context.Background(), query, args...).Scan(&num)
	if e != nil {
		if e == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("%w:%s", e, query)
	}
	return num > 0, nil
}

func (b *BaseModel) CountWhere(where string, args ...interface{}) (int64, error) {
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

func (b *BaseModel) UpdateSet(sets string, where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	query := `update ` + b.TableName + ` set ` + sets + where
	result, e := b.Pool.Exec(context.Background(), query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected(), nil
}

func (b *BaseModel) Clear() error {
	query := `truncate table ` + b.TableName
	_, e := b.Pool.Exec(context.Background(), query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel) Truncate() error {
	return b.Clear()
}

func (b *BaseModel) Delete(id interface{}) (int64, error) {
	query := `delete from ` + b.TableName + ` where ` + b.dbTags[0] + `=$1`
	result, e := b.Pool.Exec(context.Background(), query, id)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected(), nil
}

func (b *BaseModel) DeleteWhere(where string, args ...interface{}) (int64, error) {
	where = toWhere(where)

	query := `delete from ` + b.TableName + where
	result, e := b.Pool.Exec(context.Background(), query, args...)
	if e != nil {
		return 0, fmt.Errorf("%w:%s", e, query)
	}
	return result.RowsAffected(), nil
}
