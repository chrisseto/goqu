package goqu

import (
	"context"
	"database/sql"
	"sync"

	"github.com/doug-martin/goqu/v9/exec"
)

type (
	Logger interface {
		Printf(format string, v ...interface{})
	}
	SQL interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	}
	DSL struct {
		logger  Logger
		dialect string
		sql     SQL
		qf      exec.QueryFactory
		qfOnce  sync.Once
	}
)

// This is the common entry point into goqu.
//
// dialect: This is the adapter dialect, you should see your database adapter for the string to use. Built in adapters
// can be found at https://github.com/doug-martin/goqu/tree/master/adapters
//
// db: A sql.Db to use for querying the database
//      import (
//          "database/sql"
//          "fmt"
//          "github.com/doug-martin/goqu/v9"
//          _ "github.com/doug-martin/goqu/v9/dialect/postgres"
//          _ "github.com/lib/pq"
//      )
//
//      func main() {
//          sqlDb, err := sql.Open("postgres", "user=postgres dbname=goqupostgres sslmode=disable ")
//          if err != nil {
//              panic(err.Error())
//          }
//          db := goqu.New("postgres", sqlDb)
//      }
// The most commonly used Database method is From, which creates a new Dataset that uses the correct adapter and
// supports queries.
//          var ids []uint32
//          if err := db.From("items").Where(goqu.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//              panic(err.Error())
//          }
//          fmt.Printf("%+v", ids)
func newDSL(dialect string, sql SQL) *DSL {
	return &DSL{
		logger:  nil,
		dialect: dialect,
		sql:     sql,
		qf:      nil,
		qfOnce:  sync.Once{},
	}
}

// returns this databases dialect
func (d *DSL) Dialect() string {
	return d.dialect
}

// Creates a new Dataset that uses the correct adapter and supports queries.
//          var ids []uint32
//          if err := db.From("items").Where(goqu.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//              panic(err.Error())
//          }
//          fmt.Printf("%+v", ids)
//
// from...: Sources for you dataset, could be table names (strings), a goqu.Literal or another goqu.Dataset
func (d *DSL) From(from ...interface{}) *SelectDataset {
	return newDataset(d.dialect, d.queryFactory()).From(from...)
}

func (d *DSL) Select(cols ...interface{}) *SelectDataset {
	return newDataset(d.dialect, d.queryFactory()).Select(cols...)
}

func (d *DSL) Update(table interface{}) *UpdateDataset {
	return newUpdateDataset(d.dialect, d.queryFactory()).Table(table)
}

func (d *DSL) Insert(table interface{}) *InsertDataset {
	return newInsertDataset(d.dialect, d.queryFactory()).Into(table)
}

func (d *DSL) Delete(table interface{}) *DeleteDataset {
	return newDeleteDataset(d.dialect, d.queryFactory()).From(table)
}

func (d *DSL) Truncate(table ...interface{}) *TruncateDataset {
	return newTruncateDataset(d.dialect, d.queryFactory()).Table(table...)
}

// Sets the logger for to use when logging queries
func (d *DSL) Logger(logger Logger) {
	d.logger = logger
}

// Logs a given operation with the specified sql and arguments
func (d *DSL) Trace(op, sqlString string, args ...interface{}) {
	if d.logger != nil {
		if sqlString != "" {
			if len(args) != 0 {
				d.logger.Printf("[goqu] %s [query:=`%s` args:=%+v]", op, sqlString, args)
			} else {
				d.logger.Printf("[goqu] %s [query:=`%s`]", op, sqlString)
			}
		} else {
			d.logger.Printf("[goqu] %s", op)
		}
	}
}

// Uses the db to Execute the query with arguments and return the sql.Result
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) Exec(query string, args ...interface{}) (sql.Result, error) {
	return d.ExecContext(context.Background(), query, args...)
}

// Uses the db to Execute the query with arguments and return the sql.Result
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	d.Trace("EXEC", query, args...)
	return d.sql.ExecContext(ctx, query, args...)
}

// Can be used to prepare a query.
//
// You can use this in tandem with a dataset by doing the following.
//    sql, args, err := db.From("items").Where(goqu.I("id").Gt(10)).ToSQL(true)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    stmt, err := db.Prepare(sql)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer stmt.Close()
//    rows, err := stmt.Query(args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
// query: The SQL statement to prepare.
func (d *DSL) Prepare(query string) (*sql.Stmt, error) {
	return d.PrepareContext(context.Background(), query)
}

// Can be used to prepare a query.
//
// You can use this in tandem with a dataset by doing the following.
//    sql, args, err := db.From("items").Where(goqu.I("id").Gt(10)).ToSQL(true)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    stmt, err := db.Prepare(sql)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer stmt.Close()
//    rows, err := stmt.QueryContext(ctx, args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
// query: The SQL statement to prepare.
func (d *DSL) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	d.Trace("PREPARE", query)
	return d.sql.PrepareContext(ctx, query)
}

// Used to query for multiple rows.
//
// You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).ToSQL()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.Query(args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return d.QueryContext(context.Background(), query, args...)
}

// Used to query for multiple rows.
//
// You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).ToSQL()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.QueryContext(ctx, args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	d.Trace("QUERY", query, args...)
	return d.sql.QueryContext(ctx, query, args...)
}

// Used to query for a single row.
//
// You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).Limit(1).ToSQL()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.QueryRow(args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    //scan your row
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) QueryRow(query string, args ...interface{}) *sql.Row {
	return d.QueryRowContext(context.Background(), query, args...)
}

// Used to query for a single row.
//
// You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).Limit(1).ToSQL()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.QueryRowContext(ctx, args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    //scan your row
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	d.Trace("QUERY ROW", query, args...)
	return d.sql.QueryRowContext(ctx, query, args...)
}

func (d *DSL) queryFactory() exec.QueryFactory {
	d.qfOnce.Do(func() {
		d.qf = exec.NewQueryFactory(d)
	})
	return d.qf
}

// Queries the database using the supplied query, and args and uses CrudExec.ScanStructs to scan the results into a
// slice of structs
//
// i: A pointer to a slice of structs
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanStructs(i interface{}, query string, args ...interface{}) error {
	return d.ScanStructsContext(context.Background(), i, query, args...)
}

// Queries the database using the supplied context, query, and args and uses CrudExec.ScanStructsContext to scan the
// results into a slice of structs
//
// i: A pointer to a slice of structs
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanStructsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error {
	return d.queryFactory().FromSQL(query, args...).ScanStructsContext(ctx, i)
}

// Queries the database using the supplied query, and args and uses CrudExec.ScanStruct to scan the results into a
// struct
//
// i: A pointer to a struct
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	return d.ScanStructContext(context.Background(), i, query, args...)
}

// Queries the database using the supplied context, query, and args and uses CrudExec.ScanStructContext to scan the
// results into a struct
//
// i: A pointer to a struct
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanStructContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	return d.queryFactory().FromSQL(query, args...).ScanStructContext(ctx, i)
}

// Queries the database using the supplied query, and args and uses CrudExec.ScanVals to scan the results into a slice
// of primitive values
//
// i: A pointer to a slice of primitive values
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanVals(i interface{}, query string, args ...interface{}) error {
	return d.ScanValsContext(context.Background(), i, query, args...)
}

// Queries the database using the supplied context, query, and args and uses CrudExec.ScanValsContext to scan the
// results into a slice of primitive values
//
// i: A pointer to a slice of primitive values
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanValsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error {
	return d.queryFactory().FromSQL(query, args...).ScanValsContext(ctx, i)
}

// Queries the database using the supplied query, and args and uses CrudExec.ScanVal to scan the results into a
// primitive value
//
// i: A pointer to a primitive value
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	return d.ScanValContext(context.Background(), i, query, args...)
}

// Queries the database using the supplied context, query, and args and uses CrudExec.ScanValContext to scan the
// results into a primitive value
//
// i: A pointer to a primitive value
//
// query: The SQL to execute
//
// args...: for any placeholder parameters in the query
func (d *DSL) ScanValContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	return d.queryFactory().FromSQL(query, args...).ScanValContext(ctx, i)
}
