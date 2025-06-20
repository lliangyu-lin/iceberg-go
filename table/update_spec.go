// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"fmt"
	"github.com/apache/iceberg-go"
)

type UpdateSpec struct {
	txn                   *Transaction
	nameToField           map[string]iceberg.PartitionField
	nameToAddedField      map[string]iceberg.PartitionField
	transformToField      map[transformKey]iceberg.PartitionField
	transformToAddedField map[transformKey]iceberg.PartitionField
	caseSensitive         bool
	adds                  []iceberg.PartitionField
	deletes               map[int]bool
}

func NewUpdateSpec(t *Transaction, caseSensitive bool) *UpdateSpec {
	transformToField := make(map[transformKey]iceberg.PartitionField)
	partitionSpec := t.tbl.Metadata().PartitionSpec()
	for partitionField := range partitionSpec.Fields() {
		transformToField[transformKey{
			FieldID:   partitionField.SourceID,
			Transform: partitionField.Transform.String(),
		}] = partitionField
	}

	return &UpdateSpec{
		txn:                   t,
		nameToField:           make(map[string]iceberg.PartitionField),
		nameToAddedField:      make(map[string]iceberg.PartitionField),
		transformToField:      transformToField,
		transformToAddedField: make(map[transformKey]iceberg.PartitionField),
		caseSensitive:         caseSensitive,
		adds:                  make([]iceberg.PartitionField, 0),
		deletes:               make(map[int]bool),
	}
}

type transformKey struct {
	FieldID   int
	Transform string
}

func (us *UpdateSpec) AddField(sourceColName string, transform iceberg.Transform, partitionFieldName string) (*UpdateSpec, error) {
	// Finds the column in the schema and binds it with case sensitivity.
	//fmt.Println(us.transformToField)

	ref := iceberg.Reference(sourceColName)
	boundTerm, err := ref.Bind(us.txn.tbl.Schema(), us.caseSensitive)
	if err != nil {
		return nil, err
	}

	// Validate the transform
	outputType := boundTerm.Type()
	if !transform.CanTransform(outputType) {
		return nil, fmt.Errorf("%s cannot transform %s values from %s", transform.String(), outputType.String(), boundTerm.Ref().Field().Name)
	}

	// Check for duplicate transform on same column
	key := &transformKey{
		FieldID:   boundTerm.Ref().Field().ID,
		Transform: transform.String(),
	}
	existingPartitionField, exists := us.transformToField[*key]
	if exists && us.isDuplicatePartition(transform, existingPartitionField) {
		return nil, fmt.Errorf("duplicate partition field for %s=%v, %v already exists", ref.String(), ref, existingPartitionField)
	}

	// Check if this transform was already added
	//added, exists := us.transformToAddedField[*key]
	//if exists {
	//	return nil, fmt.Errorf("already added partition: %s ", added)
	//}

	// Create the new partition field and Check for name collisions
	// with existing fields
	//newField := us.partitionField(*key, partitionFieldName)
	//if _, exists = us.nameToAddedField[newField.Name]; exists {
	//	return nil, fmt.Errorf("already added partition field with name: %s", newField.Name)
	//}

	// Handle special case for time transforms

	// If name matches an existing field, rename it (if VOID)
	//us.transformToAddedField[*key] = newField
	//existingPartitionField, exists = us.nameToField[newField.Name]
	//if _, inDelete := us.deletes[existingPartitionField.FieldID]; exists && inDelete {
	//	if _, ok := existingPartitionField.Transform.(iceberg.VoidTransform); ok {
	//		_, err = us.RenameField(existingPartitionField.Name, fmt.Sprintf("%s_%d", existingPartitionField.Name, existingPartitionField.FieldID))
	//		if err != nil {
	//			return nil, err
	//		}
	//	} else {
	//		return nil, fmt.Errorf("cannot add duplicate partition field name: %s", existingPartitionField.Name)
	//	}
	//}

	// Register the new field
	//us.nameToAddedField[newField.Name] = newField
	//us.adds = append(us.adds, newField)
	return us, nil
}

func (us *UpdateSpec) RenameField(name string, newName string) (*UpdateSpec, error) {
	return nil, nil
}

func (us *UpdateSpec) Apply() *iceberg.PartitionSpec {
	spec := us.txn.tbl.Metadata().PartitionSpec()
	return &spec
}

func (us *UpdateSpec) Commit() ([]Update, []Requirement, error) {
	return nil, nil, nil
}

func (us *UpdateSpec) partitionField(key transformKey, name string) iceberg.PartitionField {
	return iceberg.PartitionField{}
}

func (us *UpdateSpec) isDuplicatePartition(transform iceberg.Transform, partitionField iceberg.PartitionField) bool {
	_, exists := us.deletes[partitionField.FieldID]
	return !exists && transform == partitionField.Transform
}
