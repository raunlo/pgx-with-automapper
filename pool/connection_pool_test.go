package pool

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/raunlo/pgx-with-automapper/mapper"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"os"
	"testing"
	"time"
)

type testUserStruct struct {
	UserId uint   `primaryKey:"id"`
	Name   string `db:"name"`
	Email  string `db:"email"`
}

var (
	connectionPool Conn
	pgContainer    *postgres.PostgresContainer
	password       = "testpass"
	port           = "5432"
	databaseUser   = "testuser"
	databaseName   = "testdb"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	pgContainer, err = postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithUsername(databaseUser),
		postgres.WithPassword(password),
		postgres.WithDatabase(databaseName),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)))
	err = pgContainer.Start(context.Background())
	if err != nil {
		panic(err)
	}

	databaseConfiguration := createDatabaseConfiguration(ctx)
	connectionPool = NewDatabasePool(*databaseConfiguration)

	// Insert test data
	_, err = connectionPool.Exec(ctx, `
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL
        );
        INSERT INTO users (name, email) VALUES
        ('John Doe', 'john.doe@example.com');
    `)
	if err != nil {
		panic(err)
	}

	code := m.Run()

	// Teardown the container
	if err := pgContainer.Terminate(ctx); err != nil {
		panic(err)
	}

	// Exit with the test result code
	os.Exit(code)
}

func createDatabaseConfiguration(ctx context.Context) *DatabaseConfiguration {
	// Extract host, port, username, and database name
	host, err := pgContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Failed to get host: %v", err)
	}

	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		log.Fatalf("Failed to get mapped port: %v", err)
	}
	portValue := port.Port()

	return &DatabaseConfiguration{
		User:     &databaseUser,
		Password: &password,
		Host:     &host,
		Port:     &portValue,
		Name:     &databaseName,
	}
}

func TestQueryOneWhichReturnsOne(t *testing.T) {
	res := testUserStruct{}
	err := connectionPool.QueryOne(context.Background(), "SELECT * FROM users WHERE ID  = 1", &res, nil)

	assert.NoError(t, err)
	assert.Equal(t, uint(1), res.UserId)
	assert.Equal(t, "John Doe", res.Name)
	assert.Equal(t, "john.doe@example.com", res.Email)
}

func TestQueryOneInTransactionWhichReturnsOne(t *testing.T) {
	res := testUserStruct{}
	tx, err := connectionPool.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.QueryOne(context.Background(), "SELECT * FROM users WHERE ID  = 1", &res, nil)

	assert.NoError(t, err)
	assert.Equal(t, uint(1), res.UserId)
	assert.Equal(t, "John Doe", res.Name)
	assert.Equal(t, "john.doe@example.com", res.Email)
	defer func(tx TransactionWrapper, ctx context.Context) {
		err := tx.Commit(ctx)
		if err != nil {
			t.Fatalf("Failed to commit test transaction: %v", err)
		}
	}(tx, context.Background())
}

func TestQueryOneWhichReturnsEmpty(t *testing.T) {
	res := testUserStruct{}
	err := connectionPool.QueryOne(context.Background(), "SELECT * FROM users WHERE ID  = 2", &res, nil)
	assert.EqualError(t, err, mapper.ErrNoRows.Error())
}

func TestQueryListReturnsList(t *testing.T) {
	var res []testUserStruct
	err := connectionPool.QueryList(context.Background(), "SELECT * FROM users", &res, nil)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, uint(1), res[0].UserId)
	assert.Equal(t, "John Doe", res[0].Name)
	assert.Equal(t, "john.doe@example.com", res[0].Email)
}

func TestQueryListInTransactionReturnsList(t *testing.T) {
	var res []testUserStruct
	tx, err := connectionPool.BeginTx(context.Background(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.QueryList(context.Background(), "SELECT * FROM users", &res, nil)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, uint(1), res[0].UserId)
	assert.Equal(t, "John Doe", res[0].Name)
	assert.Equal(t, "john.doe@example.com", res[0].Email)
}

func TestQueryListReturnsEmptyList(t *testing.T) {
	var res []testUserStruct
	err := connectionPool.QueryList(context.Background(), "SELECT * FROM users where name like '%Jane%'", &res, nil)

	assert.NoError(t, err)
	assert.Equal(t, 0, len(res))
}

func TestQueryReturnsRows(t *testing.T) {
	rows, err := connectionPool.Query(context.Background(), "SELECT * FROM users")
	assert.NoError(t, err)
	assert.Equal(t, rows.Next(), true)

	rowMap, err := pgx.RowToMap(rows)

	assert.NoError(t, err)
	assert.Equal(t, int32(1), rowMap["id"])
	assert.Equal(t, "John Doe", rowMap["name"])
	assert.Equal(t, "john.doe@example.com", rowMap["email"])
}
