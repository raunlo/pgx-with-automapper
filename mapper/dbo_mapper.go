package mapper

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	reflectutils "pgx-with-mapper/reflect_utils"
	"reflect"
	"time"
)

var (
	ErrNoRows = errors.New("no rows found")
)

func getTooManyRowsError(entityType reflect.Type) error {
	return errors.New(fmt.Sprintf("Too many rows for entity(name=%s)", entityType))
}

func analyzeEntityGraphs(entityType reflect.Type) {
	err := analyzeEntity(entityType)
	if err != nil {
		panic(err)
	}
}

func analyzeEntity(currentType reflect.Type) error {
	var fieldMapping = make(map[string]int)
	var relationships = make(map[int]reflect.Type)
	var keyField string
	if _, exists := GetEntityGraphMappingInfo(currentType); exists {
		return nil
	}

	currentType = reflectutils.DeReferencePointer(currentType)

	// set dummy value to avoid infinite recursion
	SetEntityGraphMappingInfo(currentType, nil)
	for index := 0; index < currentType.NumField(); index++ {

		field := currentType.Field(index)
		dbTag := field.Tag.Get("db")
		primaryKeyTag := field.Tag.Get("primaryKey")
		relationshipTag := field.Tag.Get("relationship")

		switch {
		case primaryKeyTag != "":
			if keyField == "" {
				fieldMapping[primaryKeyTag] = index
				keyField = primaryKeyTag
			} else {
				return errors.New("multiple primary key fields found")
			}
		case relationshipTag != "":
			relationships[index] = field.Type
			var elementType = reflectutils.DeReferencePointer(field.Type)
			if elementType.Kind() == reflect.Slice {
				elementType = elementType.Elem()
			}

			err := analyzeEntity(elementType)
			if err != nil {
				return err
			}

		case dbTag != "":
			fieldMapping[dbTag] = index

		}
	}

	mappingInfo := &MappingInfo{
		KeyField:      keyField,
		FieldMapping:  fieldMapping,
		Relationships: relationships,
	}
	SetEntityGraphMappingInfo(currentType, mappingInfo)
	return nil
}

// ScanOne scans rows into one object. Might need to scan multiple rows where there is one-to-many or one-to-one relationships
func ScanOne(rows pgx.Rows, dest interface{}) error {
	defer rows.Close()
	destinationType := reflect.TypeOf(dest)
	if destinationType == nil {
		return errors.New("dest cannot be nil")
	}

	if destinationType.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}
	// de-reference pointer
	destinationType = reflectutils.DeReferencePointer(destinationType)

	lookupEntity := make(map[reflect.Type]map[interface{}]reflect.Value)

	for rows.Next() {
		rowInMap, err := pgx.RowToMap(rows)
		if err != nil {
			return err
		}
		_, _, err = mapToStruct(destinationType, rowInMap, lookupEntity, dest)
		if err != nil {
			return err
		}
	}

	if reflect.ValueOf(dest).Elem().IsZero() {
		return ErrNoRows
	}
	return nil
}

// ScanMany scans rows into a slice of objects.
func ScanMany(rows pgx.Rows, dest interface{}) error {
	defer rows.Close()
	destinationPtrValue := reflect.ValueOf(dest)
	if dest == nil {
		return errors.New("dest cannot be nil")
	}

	if destinationPtrValue.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}

	destinationValue := destinationPtrValue.Elem()

	// de-reference pointer
	destinationType := reflectutils.DeReferencePointer(destinationValue.Type())

	if destinationType.Kind() != reflect.Slice {
		return errors.New("dest must be a slice")
	}

	elType := destinationType.Elem()

	lookupEntity := make(map[reflect.Type]map[interface{}]reflect.Value)
	result := reflect.MakeSlice(destinationType, 0, 0)
	for rows.Next() {
		newInstance := reflect.New(elType).Interface()
		rowInMap, err := pgx.RowToMap(rows)
		if err != nil {
			return err
		}

		obj, exists, err := mapToStruct(elType, rowInMap, lookupEntity, newInstance)
		if err != nil {
			return err
		}

		// Append to the existing slice only if it is not yet mapped
		if obj.IsValid() && exists != nil && !*exists {
			if obj.Kind() == reflect.Ptr {
				obj = obj.Elem()
			}
			result = reflect.Append(result, obj)
		}
	}
	destinationValue.Set(result)
	return nil
}

// Function to map database values to struct fields Returns object, if it is already mapper and error
func mapToStruct(entityType reflect.Type, values map[string]any, lookup map[reflect.Type]map[interface{}]reflect.Value,
	dest interface{}) (reflect.Value, *bool, error) {

	entityLookup, entityLookupExists := lookup[entityType]
	if !entityLookupExists {
		lookup[entityType] = make(map[interface{}]reflect.Value)
		entityLookup = lookup[entityType]
	}

	entityMappingInfo, mappingInfoExists := GetEntityGraphMappingInfo(entityType)
	if !mappingInfoExists {
		analyzeEntityGraphs(entityType)
		entityMappingInfo, mappingInfoExists = GetEntityGraphMappingInfo(entityType)
		if entityMappingInfo == nil {
			return reflect.Value{}, nil, errors.New(fmt.Sprintf("no mapping info found for entity(%s)", entityType))
		}
	}
	keyValue, keyValueExists := values[entityMappingInfo.KeyField]
	if !keyValueExists {
		return reflect.Value{}, nil, errors.New("no key field found in values")
	}

	obj, entityExists := entityLookup[keyValue]
	if !entityExists {
		// reflect_utils entity
		obj = reflect.ValueOf(dest) // obj is now a reflect_utils.Value pointing to a pointer to the struct
		objValue := obj.Elem()      // Dereference to get the actual struct
		for columnName, structIndex := range entityMappingInfo.FieldMapping {

			field := objValue.Field(structIndex)
			dbValue := values[columnName]

			if dbValue == nil {
				continue // Handle NULL values
			}

			// Convert & Set Value
			if err := setFieldValue(field, dbValue); err != nil {
				return reflect.Value{}, nil, fmt.Errorf("failed to map column %s: %w", columnName, err)
			}
		}
	}
	err := mapRelationships(entityMappingInfo, values, lookup, obj.Elem())
	if err != nil {
		return reflect.Value{}, nil, err
	}
	lookup[entityType][keyValue] = obj
	return obj, &entityExists, nil
}

// logic to handle entity relationships. This function creates struct and then appends to current struct
func mapRelationships(entityMappingInfo *MappingInfo, values map[string]any, lookup map[reflect.Type]map[interface{}]reflect.Value, obj reflect.Value) error {
	for fieldIndex, relationshipEntityType := range entityMappingInfo.Relationships {
		relationshipEntityType := reflectutils.DeReferencePointer(relationshipEntityType)

		if relationshipEntityType.Kind() == reflect.Slice {
			relationshipEntityType = relationshipEntityType.Elem()
		}

		value, _, err := mapToStruct(relationshipEntityType, values, lookup, reflect.New(relationshipEntityType).Interface())

		if err != nil {
			return err
		}
		if value.IsValid() {
			field := obj.Field(fieldIndex)
			if reflectutils.IsStruct(field) && !reflect.Indirect(field).IsZero() {
				return getTooManyRowsError(relationshipEntityType)
			}
			err = setFieldValue(field, value.Interface())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Function to Convert Database Value to Go Struct Field
func setFieldValue(field reflect.Value, value interface{}) error {

	if !field.CanSet() {
		return errors.New("field is not settable")
	}
	// if field is not pointer, but value is pointer, then dereference
	v := reflect.ValueOf(value)
	if field.Kind() != reflect.Ptr && v.Kind() == reflect.Ptr && !v.IsNil() {
		value = v.Elem().Interface()
		v = v.Elem()
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return setIntField(field, value, v)
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return setUintField(field, value, v)
	case reflect.String:
		return setStringField(field, value, v)
	case reflect.Bool:
		return setBoolField(field, value, v)
	case reflect.Float64:
		return setFloatField(field, value, v)
	case reflect.Struct:
		return setStructField(field, value, v)
	case reflect.Slice:
		return setSliceField(field, value, v)
	case reflect.Ptr:
		return setPointerField(field, v)
	default:
		return fmt.Errorf("unsupported field type: %s", field.Kind().String())
	}
}

func setPointerField(field reflect.Value, v reflect.Value) error {
	if field.IsNil() {
		// Initialize the pointer if it is nil
		field.Set(reflect.New(field.Type().Elem()))
	}

	return setFieldValue(field.Elem(), v.Interface())
}

func setStringField(field reflect.Value, value interface{}, v reflect.Value) error {
	if v.Kind() == reflect.String {
		field.SetString(v.String())
	} else {
		return fmt.Errorf("type mismatch: expected string, got %T", value)
	}
	return nil
}

func setBoolField(field reflect.Value, value interface{}, v reflect.Value) error {
	if v.Kind() == reflect.Bool {
		field.SetBool(v.Bool())
	} else {
		return fmt.Errorf("type mismatch: expected bool, got %T", value)
	}
	return nil
}

func setIntField(field reflect.Value, value interface{}, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		field.SetInt(v.Int())
	case reflect.Float64: // Allow conversion from float to int
		field.SetInt(int64(v.Float()))
	default:
		return fmt.Errorf("type mismatch: expected int64, got %T", value)
	}
	return nil
}

func setUintField(field reflect.Value, value interface{}, v reflect.Value) error {
	if v.Kind() == reflect.Int || v.Kind() == reflect.Int64 || v.Kind() == reflect.Int32 || v.Kind() == reflect.Int16 || v.Kind() == reflect.Int8 {
		intValue := v.Int()
		if intValue < 0 {
			return fmt.Errorf("cannot assign negative value %d to uint field", intValue)
		}
		field.SetUint(uint64(intValue)) // Safely convert int64 to uint64
	} else if v.Kind() == reflect.Uint || v.Kind() == reflect.Uint64 {
		field.SetUint(v.Uint()) // Directly assign uint values
	} else {
		return fmt.Errorf("type mismatch: expected uint or uint64, got %T", value)
	}
	return nil
}

func setFloatField(field reflect.Value, value interface{}, v reflect.Value) error {
	if v.Kind() == reflect.Float64 {
		field.SetFloat(v.Float())
	} else if v.Kind() == reflect.Int || v.Kind() == reflect.Int64 {
		field.SetFloat(float64(v.Int())) // Allow int -> float
	} else {
		return fmt.Errorf("type mismatch: expected float64, got %T", value)
	}
	return nil
}
func setStructField(field reflect.Value, value interface{}, v reflect.Value) error {
	if field.Type() == reflect.TypeOf(time.Time{}) {
		if v.Type() == reflect.TypeOf(time.Time{}) {
			field.Set(v)
		} else {
			return fmt.Errorf("type mismatch: expected time.Time, got %T", value)
		}
	} else {
		// Ensure assignability for other structs
		if v.Type().AssignableTo(field.Type()) || v.Elem().Type().AssignableTo(field.Type()) {
			field.Set(v)
		} else {
			return fmt.Errorf("type mismatch: expected %s, got %T", field.Type().Name(), v)
		}
	}
	return nil
}

func setSliceField(field reflect.Value, value interface{}, v reflect.Value) error {
	elemType := field.Type().Elem()
	// Ensure the value is a slice
	if v.Kind() != reflect.Slice {
		// Assuming newElem is a ChecklistItemRowDbo, but value is *ChecklistItemRowDbo
		currentSlice := field.Interface()
		if currentSlice == nil {
			// Handle the case where the slice is nil
			currentSlice = reflect.MakeSlice(field.Type(), 0, 1)
		}

		slice := reflect.ValueOf(currentSlice)
		newElem := reflect.New(elemType).Elem()

		if err := setFieldValue(newElem, value); err != nil {
			return err
		}

		// Append to the existing slice
		slice = reflect.Append(slice, newElem)
		field.Set(slice)
		return nil
	}

	// Ensure the slice type matches the field slice type (e.g., []int, []string)

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i)
		// Create a new slice element of the correct type
		newElem := reflect.New(elemType).Elem()
		if err := setFieldValue(newElem, elem.Interface()); err != nil {
			return err
		}
		field.Set(reflect.Append(field, newElem))
	}
	return nil
}
