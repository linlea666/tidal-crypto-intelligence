package datahub

// Observe the existing short SQLite connection at the driver boundary. A row
// cursor owns the connection until Close, including time spent decoding rows in
// Go. QueryContext return time alone cannot identify that occupation.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const researchTraceLimit = 4

type researchConnectionUse struct {
	ID        uint64    `json:"id"`
	Kind      string    `json:"kind"`
	Statement string    `json:"statement"`
	StartedAt time.Time `json:"startedAt"`
	ElapsedMS float64   `json:"elapsedMs"`
}
type researchConnectionSnapshot struct {
	Active    []researchConnectionUse `json:"active,omitempty"`
	Truncated bool                    `json:"truncated,omitempty"`
}
type researchConnectionTrace struct {
	mu      sync.Mutex
	seq     uint64
	active  map[uint64]researchConnectionUse
	dropped int
}

// SQL text and parameters never leave the driver wrapper. The fingerprint can
// be matched to a source query offline without publishing dataset IDs/payloads.
func researchStatementID(query string) string {
	kind := "SQL"
	fields := strings.Fields(query)
	if len(fields) > 0 {
		switch strings.ToUpper(fields[0]) {
		case "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "PRAGMA", "CREATE", "DROP", "ALTER":
			kind = strings.ToUpper(fields[0])
		}
	}
	hash := sha256.Sum256([]byte(strings.Join(fields, " ")))
	return fmt.Sprintf("%s/%x", kind, hash[:6])
}
func (t *researchConnectionTrace) start(kind, statement string) func() {
	t.mu.Lock()
	t.seq++
	id := t.seq
	tracked := len(t.active) < researchTraceLimit
	if tracked {
		if t.active == nil {
			t.active = make(map[uint64]researchConnectionUse)
		}
		t.active[id] = researchConnectionUse{ID: id, Kind: kind, Statement: statement, StartedAt: time.Now()}
	} else {
		t.dropped++
	}
	t.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			if tracked {
				delete(t.active, id)
			} else {
				t.dropped--
			}
			t.mu.Unlock()
		})
	}
}
func (t *researchConnectionTrace) snapshot() researchConnectionSnapshot {
	if t == nil {
		return researchConnectionSnapshot{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := researchConnectionSnapshot{Truncated: t.dropped > 0}
	for _, use := range t.active {
		use.ElapsedMS = float64(time.Since(use.StartedAt).Microseconds()) / 1000
		use.StartedAt = use.StartedAt.UTC()
		out.Active = append(out.Active, use)
	}
	sort.Slice(out.Active, func(i, j int) bool { return out.Active[i].ID < out.Active[j].ID })
	return out
}

type researchConnector struct {
	driver driver.Driver
	path   string
	trace  *researchConnectionTrace
}

func (c *researchConnector) Driver() driver.Driver { return c.driver }
func (c *researchConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.path)
	if err != nil {
		return nil, err
	}
	return &researchDriverConn{Conn: conn, trace: c.trace}, nil
}
func tracedResearchDatabase(path string, trace *researchConnectionTrace) (*sql.DB, error) {
	// Obtain the registered driver, including any configured hooks. sql.Open
	// does not open a connection; this temporary handle is never Pinged or used.
	base, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	d := base.Driver()
	if err = base.Close(); err != nil {
		return nil, err
	}
	return sql.OpenDB(&researchConnector{driver: d, path: path, trace: trace}), nil
}

type researchDriverConn struct {
	driver.Conn
	trace *researchConnectionTrace
}

func (c *researchDriverConn) Ping(ctx context.Context) error {
	return c.Conn.(driver.Pinger).Ping(ctx)
}
func (c *researchDriverConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}
func (c *researchDriverConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }
func (c *researchDriverConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	done := c.trace.start("exec", researchStatementID(q))
	defer done()
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func (c *researchDriverConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	done := c.trace.start("rows", researchStatementID(q))
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		done()
		return nil, err
	}
	return &researchDriverRows{Rows: rows, done: done}, nil
}
func (c *researchDriverConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *researchDriverConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	done := c.trace.start("transaction", "transaction")
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		done()
		return nil, err
	}
	return &researchDriverTx{Tx: tx, done: done}, nil
}
func (c *researchDriverConn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}
func (c *researchDriverConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	statement := researchStatementID(q)
	done := c.trace.start("prepare", statement)
	defer done()
	stmt, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return &researchDriverStmt{Stmt: stmt, statement: statement, trace: c.trace}, nil
}

type researchDriverTx struct {
	driver.Tx
	done func()
}

func (t *researchDriverTx) Commit() error   { defer t.done(); return t.Tx.Commit() }
func (t *researchDriverTx) Rollback() error { defer t.done(); return t.Tx.Rollback() }

type researchDriverStmt struct {
	driver.Stmt
	statement string
	trace     *researchConnectionTrace
}

func namedResearchValues(values []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(values))
	for i, value := range values {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return out
}
func (s *researchDriverStmt) Exec(values []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedResearchValues(values))
}
func (s *researchDriverStmt) Query(values []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedResearchValues(values))
}
func (s *researchDriverStmt) ExecContext(ctx context.Context, values []driver.NamedValue) (driver.Result, error) {
	done := s.trace.start("exec", s.statement)
	defer done()
	return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, values)
}
func (s *researchDriverStmt) QueryContext(ctx context.Context, values []driver.NamedValue) (driver.Rows, error) {
	done := s.trace.start("rows", s.statement)
	rows, err := s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, values)
	if err != nil {
		done()
		return nil, err
	}
	return &researchDriverRows{Rows: rows, done: done}, nil
}

type researchDriverRows struct {
	driver.Rows
	done func()
}

func (r *researchDriverRows) Close() error { defer r.done(); return r.Rows.Close() }

// Preserve modernc's column metadata as well as its execution/cancellation path.
func (r *researchDriverRows) ColumnTypeDatabaseTypeName(i int) string {
	return r.Rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(i)
}
func (r *researchDriverRows) ColumnTypeLength(i int) (int64, bool) {
	return r.Rows.(driver.RowsColumnTypeLength).ColumnTypeLength(i)
}
func (r *researchDriverRows) ColumnTypeNullable(i int) (bool, bool) {
	return r.Rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(i)
}
func (r *researchDriverRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	return r.Rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(i)
}
func (r *researchDriverRows) ColumnTypeScanType(i int) reflect.Type {
	return r.Rows.(driver.RowsColumnTypeScanType).ColumnTypeScanType(i)
}
