package reflect

import (
	"reflect"
	"testing"
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
