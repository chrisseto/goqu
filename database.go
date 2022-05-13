package goqu

import (
	"context"
	"database/sql"
)

type (
	// Interface for sql.DB, an interface is used so you can use with other
	// libraries such as sqlx instead of the native sql.DB
	SQLDatabase interface {
		SQL
		Begin() (*sql.Tx, error)
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	}
	// This struct is the wrapper for a Db. The struct delegates most calls to either an Exec instance or to the Db
	// passed into the constructor.
	Database struct {
		*DSL
		Db SQLDatabase
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
func newDatabase(dialect string, db SQLDatabase) *Database {
	return &Database{
		DSL: newDSL(dialect, db),
		Db:  db,
	}
}

// Starts a new Transaction.
func (d *Database) Begin() (*TxDatabase, error) {
	sqlTx, err := d.Db.Begin()
	if err != nil {
		return nil, err
	}
	tx := NewTx(d.dialect, sqlTx)
	tx.Logger(d.logger)
	return tx, nil
}

// Starts a new Transaction. See sql.DB#BeginTx for option description
func (d *Database) BeginTx(ctx context.Context, opts *sql.TxOptions) (*TxDatabase, error) {
	sqlTx, err := d.Db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	tx := NewTx(d.dialect, sqlTx)
	tx.Logger(d.logger)
	return tx, nil
}

// WithTx starts a new transaction and executes it in Wrap method
func (d *Database) WithTx(fn func(*TxDatabase) error) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	return tx.Wrap(func() error { return fn(tx) })
}

// A wrapper around a sql.Tx and works the same way as Database
type (
	// Interface for sql.Tx, an interface is used so you can use with other
	// libraries such as sqlx instead of the native sql.DB
	SQLTx interface {
		SQL
		Commit() error
		Rollback() error
	}
	TxDatabase struct {
		*DSL
		Tx SQLTx
	}
)

// Creates a new TxDatabase
func NewTx(dialect string, tx SQLTx) *TxDatabase {
	return &TxDatabase{
		DSL: newDSL(dialect, tx),
		Tx:  tx,
	}
}

// COMMIT the transaction
func (td *TxDatabase) Commit() error {
	td.Trace("COMMIT", "")
	return td.Tx.Commit()
}

// ROLLBACK the transaction
func (td *TxDatabase) Rollback() error {
	td.Trace("ROLLBACK", "")
	return td.Tx.Rollback()
}

// A helper method that will automatically COMMIT or ROLLBACK once the supplied function is done executing
//
//      tx, err := db.Begin()
//      if err != nil{
//           panic(err.Error()) // you could gracefully handle the error also
//      }
//      if err := tx.Wrap(func() error{
//          if _, err := tx.From("test").Insert(Record{"a":1, "b": "b"}).Exec(){
//              // this error will be the return error from the Wrap call
//              return err
//          }
//          return nil
//      }); err != nil{
//           panic(err.Error()) // you could gracefully handle the error also
//      }
func (td *TxDatabase) Wrap(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			_ = td.Rollback()
			panic(p)
		}
		if err != nil {
			if rollbackErr := td.Rollback(); rollbackErr != nil {
				err = rollbackErr
			}
		} else {
			if commitErr := td.Commit(); commitErr != nil {
				err = commitErr
			}
		}
	}()
	return fn()
}
