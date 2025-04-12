package mapper

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

type TestStruct struct{}

func TestConcurrencySetAndGetEntityGraphMappingInfo(t *testing.T) {
	testType := reflect.TypeOf(TestStruct{})
	expectedMapping := &MappingInfo{KeyField: "id"}

	var wg sync.WaitGroup
	numRoutines := 50

	// Concurrently store mapping info
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			SetEntityGraphMappingInfo(testType, expectedMapping)
		}()
	}

	// Concurrently read mapping info
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			GetEntityGraphMappingInfo(testType)
		}()
	}

	wg.Wait()

	// Final assertion
	result, exists := GetEntityGraphMappingInfo(testType)
	assert.True(t, exists)
	assert.Equal(t, expectedMapping, result)
}

func TestSetAndGetEntityGraphMappingInfo(t *testing.T) {
	testType := reflect.TypeOf(TestStruct{})
	expectedMapping := &MappingInfo{
		KeyField:     "id",
		FieldMapping: map[string]int{"id": 0},
	}

	// Store the mapping info
	SetEntityGraphMappingInfo(testType, expectedMapping)

	// Retrieve it
	result, exists := GetEntityGraphMappingInfo(testType)

	// Assertions
	assert.True(t, exists)
	assert.Equal(t, expectedMapping, result)
}

func TestGetEntityGraphMappingInfoWithNonExistentKey(t *testing.T) {
	nonExistentType := reflect.TypeOf(42) // Random type that was never stored

	// Try to retrieve a key that wasn't set
	result, exists := GetEntityGraphMappingInfo(nonExistentType)

	// Assertions
	assert.False(t, exists)
	assert.Nil(t, result)
}
