package sqlite

import (
	"database/sql"
	dsd "database/sql/driver"
)

const (
	MOD    string = "sqlite.go"
	DRIVER string = "sqlite3"
)

func init() {
	sql.Register(DRIVER, &driver{})
}

var _ dsd.Driver = &driver{}

type driver struct{}

func (d *driver) Open(name string) (dsd.Conn, error) { return nil, nil }

var _ dsd.Conn = &conn{}

type conn struct{}

func (c *conn) Prepare(query string) (dsd.Stmt, error) { return nil, nil }
func (c *conn) Close() error                           { return nil }
func (c *conn) Begin() (dsd.Tx, error)                 { return nil, nil }

var _ dsd.Stmt = &stmt{}

type stmt struct{}

func (s *stmt) Close() error                              { return nil }
func (s *stmt) NumInput() int                             { return -1 }
func (s *stmt) Exec(args []dsd.Value) (dsd.Result, error) { return nil, nil }
func (s *stmt) Query(args []dsd.Value) (dsd.Rows, error)  { return nil, nil }

var _ dsd.Result = &result{}

type result struct{}

func (r *result) LastInsertId() (int64, error) { return 0, nil }
func (r *result) RowsAffected() (int64, error) { return 0, nil }

var _ dsd.Rows = &rows{}

type rows struct{}

func (r *rows) Columns() []string           { return nil }
func (r *rows) Close() error                { return nil }
func (r *rows) Next(dest []dsd.Value) error { return nil }
