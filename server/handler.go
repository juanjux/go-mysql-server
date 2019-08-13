package server

import (
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var regKillCmd = regexp.MustCompile(`^kill (?:(query|connection) )?(\d+)$`)

var errConnectionNotFound = errors.NewKind("connection not found: %c")
// ErrRowTimeout will be returned if the wait for the row is longer than the connection timeout
var ErrRowTimeout = errors.NewKind("row read wait bigger than connection timeout")

// TODO parametrize
const rowsBatch = 100

// Handler is a connection handler for a SQLe engine.
type Handler struct {
	mu          sync.Mutex
	e           *sqle.Engine
	sm          *SessionManager
	c           map[uint32]*mysql.Conn
	readTimeout time.Duration
}

// NewHandler creates a new Handler given a SQLe engine.
func NewHandler(e *sqle.Engine, sm *SessionManager, rt time.Duration) *Handler {
	return &Handler{
		e:           e,
		sm:          sm,
		c:           make(map[uint32]*mysql.Conn),
		readTimeout: rt,
	}
}

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
	h.mu.Lock()
	if _, ok := h.c[c.ConnectionID]; !ok {
		h.c[c.ConnectionID] = c
	}
	h.mu.Unlock()

	logrus.Infof("NewConnection: client %v", c.ConnectionID)
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	h.sm.CloseConn(c)

	h.mu.Lock()
	delete(h.c, c.ConnectionID)
	h.mu.Unlock()

	// If connection was closed, kill only its associated queries.
	h.e.Catalog.ProcessList.KillOnlyQueries(c.ConnectionID)

	if err := h.e.Catalog.UnlockTables(nil, c.ConnectionID); err != nil {
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	}

	logrus.Infof("ConnectionClosed: client %v", c.ConnectionID)
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result) error,
) (err error) {
	ctx := h.sm.NewContextWithQuery(c, query)

	handled, err := h.handleKill(c, query)
	if err != nil {
		return err
	}

	if handled {
		return callback(&sqltypes.Result{})
	}

	start := time.Now()
	schema, rows, err := h.e.Query(ctx, query)
	defer func() {
		if q, ok := h.e.Auth.(*auth.Audit); ok {
			q.Query(ctx, time.Since(start), err)
		}
	}()

	if err != nil {
		return err
	}

	var r *sqltypes.Result
	var proccesedAtLeastOneBatch bool

	rowchan := make(chan sql.Row)
	errchan := make(chan error)
	quit := make(chan struct{})

	// This goroutine will be select{}ed giving a chance to Vitess to call the
	// handler.CloseConnection callback and enforcing the timeout if configured
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
			}
			row, err := rows.Next()
			if err != nil {
				errchan <- err
				return
			}
			rowchan <- row
		}
	}()

	// Default waitTime is one 1 minute if there is not timeout configured, in which case
	// it will loop to iterate again unless the socket died by the OS timeout or other problems.
	// If there is a timeout, it will be enforced to ensure that Vitess has a chance to
	// call Handler.CloseConnection()
	waitTime := 1 * time.Minute

	if h.readTimeout > 0 {
		waitTime = h.readTimeout
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

rowLoop:
	for {
		if r == nil {
			r = &sqltypes.Result{Fields: schemaToFields(schema)}
		}

		if r.RowsAffected == rowsBatch {
			if err := callback(r); err != nil {
				return err
			}

			r = nil
			proccesedAtLeastOneBatch = true
			continue
		}

		select {
		case err = <-errchan:
			if err == io.EOF {
				break rowLoop
			}
			return err
		case row := <-rowchan:
			outputRow, err := rowToSQL(schema, row)
			if err != nil {
				close(quit)
				return err
			}

			r.Rows = append(r.Rows, outputRow)
			r.RowsAffected++
		case <-timer.C:
			if h.readTimeout != 0 {
				// Return so Vitess can call the CloseConnection callback
				close(quit)
				return ErrRowTimeout.New()
			}
		}
		timer.Reset(waitTime)
	}

	if err := rows.Close(); err != nil {
		return err
	}

	// Even if r.RowsAffected = 0, the callback must be
	// called to update the state in the go-vitess' listener
	// and avoid returning errors when the query doesn't
	// produce any results.
	if r != nil && (r.RowsAffected == 0 && proccesedAtLeastOneBatch) {
		return nil
	}

	close(quit)
	return callback(r)
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery callback if the result does not contain any fields,
// or after the last ComQuery call completes.
func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	if sess := h.sm.session(c); sess != nil {
		return sess.WarningCount()
	}

	return 0
}

func (h *Handler) handleKill(conn *mysql.Conn, query string) (bool, error) {
	q := strings.ToLower(query)
	s := regKillCmd.FindStringSubmatch(q)
	if s == nil {
		return false, nil
	}

	id, err := strconv.ParseUint(s[2], 10, 32)
	if err != nil {
		return false, err
	}

	// KILL CONNECTION and KILL should close the connection. KILL QUERY only
	// cancels the query.
	//
	// https://dev.mysql.com/doc/refman/8.0/en/kill.html
	//
	// KILL [CONNECTION | QUERY] processlist_id
	// - KILL QUERY terminates the statement the connection is currently executing,
	// but leaves the connection itself intact.

	// - KILL CONNECTION is the same as KILL with no modifier:
	// It terminates the connection associated with the given processlist_id,
	// after terminating any statement the connection is executing.
	connID := uint32(id)
	h.e.Catalog.Kill(connID)
	if s[1] != "query" {
		logrus.Infof("kill connection: id %d", connID)

		h.mu.Lock()
		c, ok := h.c[connID]
		if ok {
			delete(h.c, connID)
		}
		h.mu.Unlock()
		if !ok {
			return false, errConnectionNotFound.New(connID)
		}

		h.sm.CloseConn(c)
		c.Close()
	}

	return true, nil
}

func rowToSQL(s sql.Schema, row sql.Row) ([]sqltypes.Value, error) {
	o := make([]sqltypes.Value, len(row))
	var err error
	for i, v := range row {
		o[i], err = s[i].Type.SQL(v)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

func schemaToFields(s sql.Schema) []*query.Field {
	fields := make([]*query.Field, len(s))
	for i, c := range s {
		var charset uint32 = mysql.CharacterSetUtf8
		if c.Type == sql.Blob {
			charset = mysql.CharacterSetBinary
		}

		fields[i] = &query.Field{
			Name:    c.Name,
			Type:    c.Type.Type(),
			Charset: charset,
		}
	}

	return fields
}
