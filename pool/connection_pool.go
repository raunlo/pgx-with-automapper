package pool

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/raunlo/pgx-with-automapper/mapper"
)

type DatabaseConfiguration struct {
	MaxOpenConns             *int           `yaml:"maxOpenConns"`
	MinOpenConns             *int           `yaml:"minOpenConns"`
	StatementCacheCapacity   *int           `yaml:"statementCacheCapacity"`
	ConnTimeout              *time.Duration `yaml:"connTimeout"`
	MaxOpenConnTTL           *time.Duration `yaml:"maxOpenConnTTL"`
	MaxIdleConnTTL           *time.Duration `yaml:"maxIdleConnTTL"`
	MaxConnLifetimeJitterTTL *time.Duration `yaml:"maxConnLifetimeJitterTTL"`
	User                     *string        `yaml:"user"`
	Password                 *string        `yaml:"password"`
	Host                     *string        `yaml:"host"`
	Port                     *string        `yaml:"port"`
	Name                     *string        `yaml:"name"`
	Schema                   *string        `yaml:"schema"`
	Sslmode                  *string        `yaml:"sslMode"`
}

func (cfg DatabaseConfiguration) getDSN() string { // nolint:gocritic
	query := make(url.Values)
	if cfg.MaxOpenConns != nil {
		query.Set("pool_max_conns", strconv.Itoa(*cfg.MaxOpenConns))
	}
	if cfg.MinOpenConns != nil {
		query.Set("pool_min_conns", strconv.Itoa(*cfg.MinOpenConns))
	}
	if cfg.MaxOpenConnTTL != nil {
		query.Set("pool_max_conn_lifetime", cfg.MaxOpenConnTTL.String())
	}
	if cfg.MaxIdleConnTTL != nil {
		query.Set("pool_max_conn_idle_time", cfg.MaxIdleConnTTL.String())
	}
	if cfg.MaxIdleConnTTL != nil {
		query.Set("pool_max_conn_idle_time", cfg.MaxIdleConnTTL.String())
	}
	if cfg.MaxConnLifetimeJitterTTL != nil {
		query.Set("pool_max_conn_lifetime_jitter", cfg.MaxConnLifetimeJitterTTL.String())
	}
	if cfg.StatementCacheCapacity != nil {
		query.Set("statement_cache_capacity", strconv.Itoa(*cfg.StatementCacheCapacity))
	}
	if cfg.Schema != nil {
		query.Set("search_path", *cfg.Schema)
	}
	if cfg.Sslmode != nil {
		query.Set("sslmode", *cfg.Sslmode)
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(*cfg.User, *cfg.Password),
		Host:     net.JoinHostPort(*cfg.Host, *cfg.Port),
		Path:     *cfg.Name,
		RawQuery: query.Encode(),
	}
	return dsn.String()
}

type Conn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	QueryOne(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error
	QueryList(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error
	Ping(ctx context.Context) error
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (TransactionWrapper, error)
}

func NewDatabasePool(cfg DatabaseConfiguration) Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.getDSN())
	if err != nil {
		panic(errors.Wrap(err, "create db conn pool"))
	}
	if err := pool.Ping(ctx); err != nil {
		panic(errors.Wrap(err, "Could not ping db"))
	}
	return &databaseConnectionPool{pool: pool}
}

// wrapper around transactions. To include twi emthods QueryOne and QueryList, which automap results.
type TransactionWrapper interface {
	// Begin starts a pseudo nested transaction.
	Begin(ctx context.Context) (pgx.Tx, error)

	// Commit commits the transaction if this is a real transaction or releases the savepoint if this is a pseudo nested
	// transaction. Commit will return an error where errors.Is(ErrTxClosed) is true if the Tx is already closed, but is
	// otherwise safe to call multiple times. If the commit fails with a rollback status (e.g. the transaction was already
	// in a broken state) then an error where errors.Is(ErrTxCommitRollback) is true will be returned.
	Commit(ctx context.Context) error

	// Rollback rolls back the transaction if this is a real transaction or rolls back to the savepoint if this is a
	// pseudo nested transaction. Rollback will return an error where errors.Is(ErrTxClosed) is true if the Tx is already
	// closed, but is otherwise safe to call multiple times. Hence, a defer tx.Rollback() is safe even if tx.Commit() will
	// be called first in a non-error condition. Any other failure of a real transaction will result in the connection
	// being closed.
	Rollback(ctx context.Context) error

	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	LargeObjects() pgx.LargeObjects

	Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)

	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	// Conn returns the underlying *Conn that on which this transaction is executing.
	Conn() *pgx.Conn

	// QueryOne Query one and map it into struct
	QueryOne(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error
	// QueryList Query list and map it into list of structs
	QueryList(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error
}

type transactionWrapper struct {
	tx pgx.Tx
}

func (t *transactionWrapper) Begin(ctx context.Context) (pgx.Tx, error) {
	return t.tx.Begin(ctx)
}

func (t *transactionWrapper) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *transactionWrapper) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func (t *transactionWrapper) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return t.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (t *transactionWrapper) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.tx.SendBatch(ctx, b)
}
func (t *transactionWrapper) LargeObjects() pgx.LargeObjects {
	return t.tx.LargeObjects()
}

func (t *transactionWrapper) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return t.tx.Prepare(ctx, name, sql)
}

func (t *transactionWrapper) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return t.tx.Exec(ctx, sql, arguments...)
}

func (t *transactionWrapper) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

func (t *transactionWrapper) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

// Conn returns the underlying *Conn that on which this transaction is executing.
func (t *transactionWrapper) Conn() *pgx.Conn {
	return t.tx.Conn()
}

func (t *transactionWrapper) QueryOne(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error {
	rows, err := t.Query(ctx, sql, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	return mapper.ScanOne(rows, dest)
}

func (t *transactionWrapper) QueryList(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error {
	rows, err := t.Query(ctx, sql, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	return mapper.ScanMany(rows, dest)
}

type databaseConnectionPool struct {
	pool *pgxpool.Pool
}

func (p *databaseConnectionPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.pool.QueryRow(ctx, sql, args...)
}

func (p *databaseConnectionPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.pool.Query(ctx, sql, args...)
}

func (p *databaseConnectionPool) QueryOne(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error {
	rows, err := p.pool.Query(ctx, sql, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	return mapper.ScanOne(rows, dest)
}

func (p *databaseConnectionPool) QueryList(ctx context.Context, sql string, dest interface{}, args pgx.NamedArgs) error {
	rows, err := p.pool.Query(ctx, sql, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	return mapper.ScanMany(rows, dest)
}

func (p *databaseConnectionPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.pool.Exec(ctx, sql, args...)
}

func (p *databaseConnectionPool) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *databaseConnectionPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (TransactionWrapper, error) {
	tx, err := p.pool.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	return &transactionWrapper{tx: tx}, nil
}
