package px

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Column struct {
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
	IsNullable string `db:"is_nullable"`
}

// select column_name,data_type from information_schema.columns where table_catalog='langenius' and table_schema='public' and table_name='student'
func DescTable(pool *pgxpool.Pool, database, schema, tableName string) ([]Column, error) {
	rows, e := pool.Query(context.Background(), `select column_name,data_type,is_nullable from information_schema.columns where table_catalog=$1 and table_schema=$2 and table_name=$3`, database, schema, tableName)
	if e != nil {
		return nil, e
	}

	out := []Column{}
	for rows.Next() {
		v := Column{}
		e = rows.Scan(&v.ColumnName, &v.DataType, &v.IsNullable)
		if e != nil {
			break
		}
		out = append(out, v)
	}

	//check err
	rows.Close()
	if e = rows.Err(); e != nil {
		return nil, e
	}

	return out, nil
}
