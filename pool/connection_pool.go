package pool

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"net"
	"net/url"
	"pgx-with-mapper/mapper"
	"strconv"
	"time"
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
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
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

func (p *databaseConnectionPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return p.pool.BeginTx(ctx, txOptions)
}
