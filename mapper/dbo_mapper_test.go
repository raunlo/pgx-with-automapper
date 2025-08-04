package mapper

import (
	"context"
	"reflect"
	"testing"

	"github.com/pashagolub/pgxmock/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type user struct {
	UserId uint   `primaryKey:"user_id"`
	Name   string `db:"user_name"`
}

func setupPostgresMock(t *testing.T, mockQuery string, rows [][]interface{}, columns []string) pgxmock.PgxConnIface {
	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("unexpected error opening mock DB: %s", err)
	}
	mock.ExpectQuery(mockQuery).WillReturnRows(mock.NewRows(columns).AddRows(rows...))
	return mock
}

func TestScanOne(t *testing.T) {
	runSuccessfulTest := func(inputType reflect.Type, setupTestFn func() pgxmock.PgxConnIface, query string, expectedResult any) {
		mock := setupTestFn()
		rows, err := mock.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("unexpected error querying mock DB: %s", err)
		}
		value := reflect.New(inputType).Interface()
		err = ScanOne(rows, value)

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, value)
	}

	t.Run("Maps pgx.Rows to single entity", func(t *testing.T) {
		const query = "SELECT * FROM users"
		setupFn := func() pgxmock.PgxConnIface {
			return setupPostgresMock(t, "^SELECT (.+) FROM users$",
				[][]interface{}{{1, "John"}},
				[]string{"user_id", "user_name"},
			)
		}
		expectedResult := &user{UserId: 1, Name: "John"}
		runSuccessfulTest(reflect.TypeOf(user{}), setupFn, query, expectedResult)
	})

	t.Run("When no rows are returned  ErrNoRows is returned", func(t *testing.T) {
		mock := setupPostgresMock(t, "^SELECT (.+) FROM users$", [][]interface{}{}, []string{"user_id", "user_name"})
		rows, err := mock.Query(context.Background(), "SELECT * FROM users")
		assert.NoError(t, err)

		var u user
		err = ScanOne(rows, &u)
		assert.Error(t, err, ErrNoRows)
		assert.Empty(t, u)
	})

	t.Run("Throws error if more than one row is returned when it expects only one result", func(t *testing.T) {
		const query = "SELECT * FROM address a LEFT JOIN users u on u.user_id = a.user_id"
		type address struct {
			AddressId uint   `primaryKey:"address_id"`
			Street    string `db:"address_street"`
			House     string `db:"address_house"`
			Owner     *user  `relationship:"oneToOne"`
		}

		setupMultipleUsersForAddress := func() pgxmock.PgxConnIface {
			return setupPostgresMock(t, "^SELECT (.+) FROM address a LEFT JOIN users u on u.user_id = a.user_id$",
				[][]interface{}{{1, "John", 1, "Calle", "5"}, {2, "Jane", 2, "CT", "5"}},
				[]string{"user_id", "user_name", "address_id", "address_street", "address_house"},
			)
		}

		runAndAssertTooManyRowsTest := func(setupFn func() pgxmock.PgxConnIface, query string, expectedError error) {
			mock := setupFn()
			rows, err := mock.Query(context.Background(), query)
			if err != nil {
				t.Fatalf("unexpected error querying mock DB: %s", err)
			}
			err = ScanOne(rows, &address{})
			assert.EqualError(t, err, expectedError.Error())
		}

		runAndAssertTooManyRowsTest(setupMultipleUsersForAddress, query, errors.New("Too many rows for entity(name=mapper.user)"))

	})

	t.Run("Maps pgx.Rows to entity with one-to-one pointer relationship", func(t *testing.T) {
		const query = "SELECT * FROM address a LEFT JOIN users u on u.user_id = a.user_id"
		type addressWithoutPointer struct {
			AddressId uint   `primaryKey:"address_id"`
			Street    string `db:"address_street"`
			House     string `db:"address_house"`
			Owner     user   `relationship:"oneToOne"`
		}

		type addressWithPointer struct {
			AddressId uint   `primaryKey:"address_id"`
			Street    string `db:"address_street"`
			House     string `db:"address_house"`
			Owner     *user  `relationship:"oneToOne"`
		}

		expectedAddressWithoutUserPointer := &addressWithoutPointer{AddressId: 1, Street: "Street", House: "5", Owner: user{UserId: 1, Name: "John"}}
		expectedAddressWithUserPointer := &addressWithPointer{AddressId: 1, Street: "Street", House: "5", Owner: &user{UserId: 1, Name: "John"}}

		setupFn := func() pgxmock.PgxConnIface {
			return setupPostgresMock(t, "^SELECT (.+) FROM address a LEFT JOIN users u on u.user_id = a.user_id$",
				[][]interface{}{{1, "John", 1, "Street", "5"}},
				[]string{"user_id", "user_name", "address_id", "address_street", "address_house"},
			)
		}

		runSuccessfulTest(reflect.TypeOf(addressWithoutPointer{}), setupFn, query, expectedAddressWithoutUserPointer)
		runSuccessfulTest(reflect.TypeOf(addressWithPointer{}), setupFn, query, expectedAddressWithUserPointer)

	})

	t.Run("Maps pgx.Rows to entity with one-to-many relationship", func(t *testing.T) {
		const query = "SELECT * FROM address a LEFT JOIN users u on u.user_id = a.user_id"
		type addressWithoutUsersSlicePointer struct {
			AddressId uint   `primaryKey:"address_id"`
			Street    string `db:"address_street"`
			House     string `db:"address_house"`
			Owners    []user `relationship:"oneToMany"`
		}

		type addressWithUsersSlicePointer struct {
			AddressId uint    `primaryKey:"address_id"`
			Street    string  `db:"address_street"`
			House     string  `db:"address_house"`
			Owners    *[]user `relationship:"oneToMany"`
		}
		usersSlice := []user{{UserId: 1, Name: "John"}, {UserId: 2, Name: "Jane"}}
		expectedAddressWithoutUserPointer := &addressWithoutUsersSlicePointer{AddressId: 1, Street: "Street", House: "5", Owners: usersSlice}
		expectedAddressWithUserPointer := &addressWithUsersSlicePointer{AddressId: 1, Street: "Street", House: "5", Owners: &usersSlice}

		setup := func() pgxmock.PgxConnIface {
			return setupPostgresMock(t, "^SELECT (.+) FROM address a LEFT JOIN users u on u.user_id = a.user_id$",
				[][]interface{}{{1, "John", 1, "Street", "5"}, {2, "Jane", 1, "Street", "5"}},
				[]string{"user_id", "user_name", "address_id", "address_street", "address_house"},
			)
		}

		runSuccessfulTest(reflect.TypeOf(addressWithoutUsersSlicePointer{}), setup, query, expectedAddressWithoutUserPointer)
		runSuccessfulTest(reflect.TypeOf(addressWithUsersSlicePointer{}), setup, query, expectedAddressWithUserPointer)

	})

	t.Run("Fails to map pgx.Rows to Entity when input is not pointer to instance", func(t *testing.T) {
		mock := setupPostgresMock(t, "^SELECT (.+) FROM users$", [][]interface{}{}, []string{"user_id", "user_name"})
		rows, err := mock.Query(context.Background(), "SELECT * FROM users")
		assert.NoError(t, err)

		var u []user
		err = ScanOne(rows, u)
		assert.ErrorContains(t, err, "dest must be a pointer")

		err = ScanOne(rows, &u)
		assert.ErrorContains(t, err, "dest must be a pointer to a struct")

		err = ScanOne(rows, nil)
		assert.ErrorContains(t, err, "dest cannot be nil")
	})
}

func TestScanMany(t *testing.T) {
	runSuccessfulTest := func(inputType reflect.Type, setupTestFn func() pgxmock.PgxConnIface, query string, expectedResult any) {
		mock := setupTestFn()
		rows, err := mock.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("unexpected error querying mock DB: %s", err)
		}
		value := reflect.New(inputType).Interface()
		err = ScanMany(rows, value)

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, value)
	}
	t.Run("Maps pgx.Rows to multiple entities", func(t *testing.T) {
		setupFn := func() pgxmock.PgxConnIface {
			return setupPostgresMock(t, "^SELECT (.+) FROM users$",
				[][]interface{}{{1, "John"}, {2, "Jane"}}, []string{"user_id", "user_name"})
		}

		expectedResult := []user{{UserId: 1, Name: "John"}, {UserId: 2, Name: "Jane"}}
		runSuccessfulTest(reflect.TypeOf([]user{}), setupFn, "SELECT * FROM users", &expectedResult)
	})

	t.Run("Maps pgx.Rows to multiple entities with one-to-one relationship", func(t *testing.T) {
		// org dataset which pints to user and datatype is a pointer
		const query = "SELECT * FROM org o left join users u on u.user_id = o.user_id"
		type OrgWithUserPointer struct {
			OrgId   uint   `primaryKey:"org_id"`
			OrgName string `db:"org_name"`
			User    *user  `relationship:"oneToOne"`
		}
		// org dataset which pints to user and datatype is object not pointer
		type OrgWithUser struct {
			OrgId   uint   `primaryKey:"org_id"`
			OrgName string `db:"org_name"`
			User    user   `relationship:"oneToOne"`
		}

		u := user{UserId: 1, Name: "John"}
		expectedOrgWithUserResult := &[]OrgWithUser{
			{User: u, OrgId: 1, OrgName: "default_org"},
			{User: u, OrgId: 2, OrgName: "billing_org"},
		}
		expectedOrgWithUserPointerResult := &[]OrgWithUserPointer{
			{User: &u, OrgId: 1, OrgName: "default_org"},
			{User: &u, OrgId: 2, OrgName: "billing_org"},
		}

		setupMockFunc := func() pgxmock.PgxConnIface {
			return setupPostgresMock(t, "^SELECT (.+) FROM org o left join users u on u.user_id = o.user_id$",
				[][]interface{}{{1, "John", 1, "default_org"}, {1, "John", 2, "billing_org"}}, []string{"user_id", "user_name", "org_id", "org_name"})
		}

		runSuccessfulTest(reflect.TypeOf([]OrgWithUser{}), setupMockFunc, query, expectedOrgWithUserResult)
		runSuccessfulTest(reflect.TypeOf([]OrgWithUserPointer{}), setupMockFunc, query, expectedOrgWithUserPointerResult)
	})

}

func TestQueryWith_LeftJoinManyMatches(t *testing.T) {
	// org dataset which pints to user and datatype is object not pointer
	type OrgWithManyUsers struct {
		OrgId   uint   `primaryKey:"org_id"`
		OrgName string `db:"org_name"`
		Users   []user `relationship:"oneToMany"`
	}
	t.Run("Maps many results without left join matches", func(t *testing.T) {
		mock := setupPostgresMock(t, "^SELECT (.+) FROM org o left join users u on u.user_id = o.user_id$",
			[][]interface{}{{nil, nil, 1, "default_org"}}, []string{"user_id", "user_name", "org_id", "org_name"})

		expectedResult := []OrgWithManyUsers{{
			OrgId:   1,
			OrgName: "default_org",
			Users:   nil,
		}}
		rows, err := mock.Query(t.Context(), "SELECT * FROM org o left join users u on u.user_id = o.user_id")
		assert.NoError(t, err)
		var result []OrgWithManyUsers

		err = ScanMany(rows, &result)

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})

	t.Run("Maps one result without left join macthes", func(t *testing.T) {
		mock := setupPostgresMock(t, "^SELECT (.+) FROM org o left join users u on u.user_id = o.user_id$",
			[][]interface{}{{nil, nil, 1, "default_org"}}, []string{"user_id", "user_name", "org_id", "org_name"})

		expectedResult := OrgWithManyUsers{
			OrgId:   1,
			OrgName: "default_org",
			Users:   nil,
		}
		rows, err := mock.Query(t.Context(), "SELECT * FROM org o left join users u on u.user_id = o.user_id")
		assert.NoError(t, err)
		var result OrgWithManyUsers

		err = ScanOne(rows, &result)

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})
}
