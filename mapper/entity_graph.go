package mapper

import (
	"reflect"
	"sync"
)

type MappingInfo struct {
	KeyField      string               // Primary key field
	FieldMapping  map[string]int       // Maps db column name -> struct field index
	Relationships map[int]reflect.Type // Maps struct field index -> relationship struct type
}

var (
	globalEntityGraphMappingInfo = sync.Map{}
)

func GetEntityGraphMappingInfo(key reflect.Type) (*MappingInfo, bool) {
	value, exists := globalEntityGraphMappingInfo.Load(key)
	if !exists {
		return nil, false
	}
	return value.(*MappingInfo), true
}

func SetEntityGraphMappingInfo(key reflect.Type, value *MappingInfo) {
	globalEntityGraphMappingInfo.Store(key, value)
}
