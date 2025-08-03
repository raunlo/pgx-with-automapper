package reflect

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeReferencePointer(t *testing.T) {
	var intPtr *int
	intType := reflect.TypeOf(intPtr)
	derefType := DeReferencePointer(intType)
	if derefType.Kind() != reflect.Int {
		t.Errorf("Expected kind %v, got %v", reflect.Int, derefType.Kind())
	}

	intType = reflect.TypeOf(0)
	derefType = DeReferencePointer(intType)
	if derefType.Kind() != reflect.Int {
		t.Errorf("Expected kind %v, got %v", reflect.Int, derefType.Kind())
	}
}

func Test_IsStructPointerWithNonZeroFields(t *testing.T) {
	type user struct {
		Name   string
		Active bool //nolint:unused // Suppress U1000 from staticcheck
		age    int  //nolint:unused // Suppress U1000 from staticcheck
	}

	user1 := &user{}              // all fields zero → should return false
	user2 := &user{Name: "Alice"} // one field set → should return true

	v1 := reflect.ValueOf(user1)
	v2 := reflect.ValueOf(user2)

	assert.False(t, IsStructPointerWithNonZeroFields(v1))
	assert.True(t, IsStructPointerWithNonZeroFields(v2))
}
