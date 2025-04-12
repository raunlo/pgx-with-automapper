package reflect

import "reflect"

func DeReferencePointer(v reflect.Type) reflect.Type {
	if v.Kind() == reflect.Ptr {
		return v.Elem()
	}
	return v
}

func IsStruct(v reflect.Value) bool {
	return v.Kind() == reflect.Struct || (v.Kind() == reflect.Ptr && v.Elem().Kind() == reflect.Struct)
}
