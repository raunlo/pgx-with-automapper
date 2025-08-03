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

func IsStructPointerWithNonZeroFields(v reflect.Value) bool {
	if !v.IsValid() || v.Kind() != reflect.Ptr || v.IsNil() {
		return false
	}

	elem := v.Elem()
	if elem.Kind() != reflect.Struct {
		return false
	}

	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if !field.IsZero() {
			return true // ✅ Found at least one meaningful field
		}
	}

	return false // ❌ All fields were default/zero
}
