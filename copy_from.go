package pgx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
)

// CopyFromRows returns a CopyFromSource interface over the provided rows slice
// making it usable by *Conn.CopyFrom.
func CopyFromRows(rows [][]interface{}) CopyFromSource {
	return &copyFromRows{rows: rows, idx: -1}
}

type copyFromRows struct {
	rows [][]interface{}
	idx  int
}

func (ctr *copyFromRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyFromRows) Values() ([]interface{}, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

func (ctr *copyFromRows) OIDs() []uint32 {
	if len(ctr.rows) == 0 {
		return nil
	}
	ci := pgtype.NewConnInfo()
	uids := make([]uint32, len(ctr.rows[0]))
	for i, v := range ctr.rows[0] {
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		typ, ok := ci.DataTypeForValue(reflect.New(t).Interface())
		if !ok {
			return nil
		}
		uids[i] = typ.OID
	}
	return uids
}

// CopyFromSlice returns a CopyFromSource interface over a dynamic func
// making it usable by *Conn.CopyFrom.
func CopyFromSlice(length int, next func(int) ([]interface{}, error)) CopyFromSource {
	return &copyFromSlice{next: next, idx: -1, len: length}
}

type copyFromSlice struct {
	next func(int) ([]interface{}, error)
	idx  int
	len  int
	err  error
}

func (cts *copyFromSlice) Next() bool {
	cts.idx++
	return cts.idx < cts.len
}

func (cts *copyFromSlice) Values() ([]interface{}, error) {
	values, err := cts.next(cts.idx)
	if err != nil {
		cts.err = err
	}
	return values, err
}

func (cts *copyFromSlice) Err() error {
	return cts.err
}

func (ctr *copyFromSlice) OIDs() []uint32 {
	return nil
}

// CopyFromSource is the interface used by *Conn.CopyFrom as the source for copy data.
type CopyFromSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). When there are no more rows available or an error
	// has occurred it returns false.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]interface{}, error)

	// OIDs returns field OIDs to build value buffers.
	// If this returns nil, then *Conn.CopyFrom will fire a prepared statement to get the field OIDs.
	OIDs() []uint32

	// Err returns any error that has been encountered by the CopyFromSource. If
	// this is not nil *Conn.CopyFrom will abort the copy.
	Err() error
}

type copyFrom struct {
	conn          *Conn
	tableName     Identifier
	columnNames   []string
	rowSrc        CopyFromSource
	readerErrChan chan error
}

func (ct *copyFrom) run(ctx context.Context) (int64, error) {
	quotedTableName := ct.tableName.Sanitize()
	cbuf := &bytes.Buffer{}
	for i, cn := range ct.columnNames {
		if i != 0 {
			cbuf.WriteString(", ")
		}
		cbuf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := cbuf.String()

	oids := ct.rowSrc.OIDs()
	if len(oids) == 0 {
		sd, err := ct.conn.Prepare(ctx, "", fmt.Sprintf("select %s from %s", quotedColumnNames, quotedTableName))
		if err != nil {
			return 0, err
		}
		oids = make([]uint32, len(sd.Fields))
		for i, f := range sd.Fields {
			oids[i] = f.DataTypeOID
		}
	}

	r, w := io.Pipe()
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)

		// Purposely NOT using defer w.Close(). See https://github.com/golang/go/issues/24283.
		buf := ct.conn.wbuf

		buf = append(buf, "PGCOPY\n\377\r\n\000"...)
		buf = pgio.AppendInt32(buf, 0)
		buf = pgio.AppendInt32(buf, 0)

		moreRows := true
		for moreRows {
			var err error
			moreRows, buf, err = ct.buildCopyBuf(buf, oids)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			if ct.rowSrc.Err() != nil {
				w.CloseWithError(ct.rowSrc.Err())
				return
			}

			if len(buf) > 0 {
				_, err = w.Write(buf)
				if err != nil {
					w.Close()
					return
				}
			}

			buf = buf[:0]
		}

		w.Close()
	}()

	startTime := time.Now()

	commandTag, err := ct.conn.pgConn.CopyFrom(ctx, r, fmt.Sprintf("copy %s ( %s ) from stdin binary;", quotedTableName, quotedColumnNames))

	r.Close()
	<-doneChan

	rowsAffected := commandTag.RowsAffected()
	if err == nil {
		if ct.conn.shouldLog(LogLevelInfo) {
			endTime := time.Now()
			ct.conn.log(ctx, LogLevelInfo, "CopyFrom", map[string]interface{}{"tableName": ct.tableName, "columnNames": ct.columnNames, "time": endTime.Sub(startTime), "rowCount": rowsAffected})
		}
	} else if ct.conn.shouldLog(LogLevelError) {
		ct.conn.log(ctx, LogLevelError, "CopyFrom", map[string]interface{}{"err": err, "tableName": ct.tableName, "columnNames": ct.columnNames})
	}

	return rowsAffected, err
}

func (ct *copyFrom) buildCopyBuf(buf []byte, oids []uint32) (bool, []byte, error) {

	for ct.rowSrc.Next() {
		values, err := ct.rowSrc.Values()
		if err != nil {
			return false, nil, err
		}
		if len(values) != len(ct.columnNames) {
			return false, nil, fmt.Errorf("expected %d values, got %d values", len(ct.columnNames), len(values))
		}

		buf = pgio.AppendInt16(buf, int16(len(ct.columnNames)))
		for i, val := range values {
			buf, err = encodePreparedStatementArgument(ct.conn.connInfo, buf, oids[i], val)
			if err != nil {
				return false, nil, err
			}
		}

		if len(buf) > 65536 {
			return true, buf, nil
		}
	}

	return false, buf, nil
}

// CopyFrom uses the PostgreSQL copy protocol to perform bulk data insertion.
// It returns the number of rows copied and an error.
//
// CopyFrom requires all values use the binary format. Almost all types
// implemented by pgx use the binary format by default. Types implementing
// Encoder can only be used if they encode to the binary format.
func (c *Conn) CopyFrom(ctx context.Context, tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int64, error) {
	ct := &copyFrom{
		conn:          c,
		tableName:     tableName,
		columnNames:   columnNames,
		rowSrc:        rowSrc,
		readerErrChan: make(chan error),
	}

	return ct.run(ctx)
}
