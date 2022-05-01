package sqlite

/*
#cgo LDFLAGS: -lsqlite3

#include <stdlib.h>
#include <sqlite3.h>

// typedef void (*sqlite3_destructor_type)(void*);
// #define SQLITE_STATIC      ((sqlite3_destructor_type)0)
// #define SQLITE_TRANSIENT   ((sqlite3_destructor_type)-1)

static int my_bind_text(sqlite3_stmt *stmt, int n, char *p, int np) {
	return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}

static int my_bind_empty_text(sqlite3_stmt *stmt, int n) {
	return sqlite3_bind_text(stmt, n, "", 0, SQLITE_STATIC);
}

static int my_bind_blob(sqlite3_stmt *stmt, int n, void *p, int np) {
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}

*/
import "C"

import (
	"database/sql"
	dsd "database/sql/driver"
	"fmt"
	"io"
	"time"
	"unsafe"
)

const (
	MOD      string = "sqlite.go"
	DRIVER   string = "sqlite3"
	TIME_FMT string = "2006-01-02 15:04:05.999999999"
)

func init() {
	sql.Register(DRIVER, &driver{})
}

var _ dsd.Driver = &driver{}

type driver struct{}

func (d *driver) Open(name string) (dsd.Conn, error) {
	var cdb *C.sqlite3

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	rc := C.sqlite3_open_v2(
		cname,
		&cdb,
		C.SQLITE_OPEN_CREATE|C.SQLITE_OPEN_READWRITE,
		nil,
	)
	if rc != C.SQLITE_OK {
		return nil, fmt.Errorf("C.sqlite3_open_v2 errno[%d]", rc)
	}
	return &conn{cdb: cdb}, nil
}

var _ dsd.Conn = &conn{}

type conn struct {
	cdb *C.sqlite3
}

func (c *conn) Prepare(query string) (dsd.Stmt, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	var cstmt *C.sqlite3_stmt
	var ctail *C.char
	rc := C.sqlite3_prepare_v2(
		c.cdb,
		cquery,
		C.int(len(query)),
		&cstmt,
		&ctail,
	)
	if rc != C.SQLITE_OK {
		return nil, fmt.Errorf("C.sqlite3_prepare_v2 errno[%d]", rc)
	}
	return &stmt{
		conn:  c,
		cstmt: cstmt,
	}, nil
}

func (c *conn) Close() error {
	rc := C.sqlite3_close(c.cdb)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("C.sqlite3_close errno[%d]", rc)
	}
	return nil
}

func (c *conn) Begin() (dsd.Tx, error) {
	return nil, fmt.Errorf("Not Implemented")
}

var _ dsd.Stmt = &stmt{}

type stmt struct {
	conn  *conn
	cstmt *C.sqlite3_stmt

	colNames     []string
	colDeclTypes []string
}

func (s *stmt) Close() error {
	rc := C.sqlite3_finalize(s.cstmt)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("C.sqlite3_finalize errno[%d]", rc)
	}
	return nil
}

func (s *stmt) NumInput() int {
	return int(C.sqlite3_bind_parameter_count(s.cstmt))
}

func (s *stmt) Exec(args []dsd.Value) (dsd.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	rc := C.sqlite3_step(s.cstmt)
	if rc != C.SQLITE_DONE {
		return nil, fmt.Errorf("C.sqlite3_finalize errno[%d]", rc)
	}

	id := int64(C.sqlite3_last_insert_rowid(s.conn.cdb))
	rows := int64(C.sqlite3_changes(s.conn.cdb))
	return &result{
		id:   id,
		rows: rows,
	}, nil
}

func (s *stmt) Query(args []dsd.Value) (dsd.Rows, error) {
	err := s.bind(args)
	if err != nil {
		return nil, err
	}

	colN := int(C.sqlite3_column_count(s.cstmt))
	s.colNames = make([]string, colN)
	s.colDeclTypes = make([]string, colN)
	for i := 0; i < int(colN); i++ {
		s.colNames[i] = C.GoString(C.sqlite3_column_name(s.cstmt, C.int(i)))
		s.colDeclTypes[i] = C.GoString(C.sqlite3_column_decltype(s.cstmt, C.int(i)))
	}
	return &rows{s: s}, nil
}

func (s *stmt) bind(args []dsd.Value) error {
	ni := s.NumInput()
	argN := len(args)
	if argN != ni {
		return fmt.Errorf("sql params count invalid, expected: %d, actural: %d", ni, argN)
	}

	for i, v := range args {
		var cc C.int = -1
		index := C.int(i + 1)
		switch v := v.(type) {
		case nil:
			cc = C.sqlite3_bind_null(s.cstmt, index)
		case int64:
			cc = C.sqlite3_bind_int64(s.cstmt, index, C.sqlite3_int64(v))
		case float64:
			cc = C.sqlite3_bind_double(s.cstmt, index, C.double(v))
		case bool:
			var vi int
			if v {
				vi = 1
			}
			cc = C.sqlite3_bind_int(s.cstmt, index, C.int(vi))
		case []byte:
			var p *byte
			vl := len(v)
			if vl > 0 {
				p = &v[0]
			}
			cc = C.my_bind_blob(s.cstmt, index, unsafe.Pointer(p), C.int(vl))
		case string:
			cc = bindString(s.cstmt, index, v)
		case time.Time:
			str := v.UTC().Format(TIME_FMT)
			cc = bindString(s.cstmt, index, str)
		default:
			cc = bindString(s.cstmt, index, fmt.Sprint(v))
		}

		if cc != 0 {
			return fmt.Errorf("C.sqlite3_bind_xxx errno[%d]", cc)
		}
	}
	return nil
}

func bindString(cstmt *C.sqlite3_stmt, index C.int, str string) C.int {
	if len(str) == 0 {
		return C.my_bind_empty_text(cstmt, index)
	}
	cstr := C.CString(str)
	defer C.free(unsafe.Pointer(cstr))
	return C.my_bind_text(cstmt, index, cstr, C.int(len(str)))
}

var _ dsd.Result = &result{}

type result struct {
	id   int64
	rows int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.rows, nil
}

var _ dsd.Rows = &rows{}

type rows struct {
	s *stmt
}

func (r *rows) Columns() []string { return r.s.colNames }
func (r *rows) Close() error      { return nil }

func (r *rows) Next(dest []dsd.Value) error {
	stmt := r.s
	cstmt := stmt.cstmt
	rc := C.sqlite3_step(cstmt)
	if rc == C.SQLITE_DONE {
		return io.EOF
	}
	if rc != C.SQLITE_ROW {
		return fmt.Errorf("C.sqlite3_step errno[%d]", rc)
	}

	for i := 0; i < len(dest); i++ {
		switch typ := C.sqlite3_column_type(cstmt, C.int(i)); typ {
		case C.SQLITE_NULL:
			dest[i] = nil
		case C.SQLITE_FLOAT:
			dest[i] = float64(C.sqlite3_column_double(cstmt, C.int(i)))
		case C.SQLITE_INTEGER:
			dest[i] = int64(C.sqlite3_column_int64(cstmt, C.int(i)))
		case C.SQLITE_TEXT, C.SQLITE_BLOB:
			n := (C.sqlite3_column_bytes(cstmt, C.int(i)))
			if n < 0 {
				dest[i] = ""
				break
			}
			p := C.sqlite3_column_blob(cstmt, C.int(i))
			b := C.GoBytes(unsafe.Pointer(p), C.int(n))
			dest[i] = b
		default:
			return fmt.Errorf("unexpected sqlite3 column type %d", typ)
		}
	}
	return nil
}
