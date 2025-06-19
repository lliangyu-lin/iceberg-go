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
	caseSensitive         bool
	transformToField      map[transformKey]iceberg.PartitionField
	transformToAddedField map[int]string
}

func NewUpdateSpec(t *Transaction, caseSensitive bool) *UpdateSpec {
	transformToField := make(map[transformKey]iceberg.PartitionField)
	return &UpdateSpec{
		txn:                   t,
		caseSensitive:         caseSensitive,
		transformToField:      transformToField,
		transformToAddedField: make(map[int]string),
	}
}

type transformKey struct {
	FieldID   int
	Transform string
}

func (us *UpdateSpec) AddField(sourceColName string, transform iceberg.Transform) (*UpdateSpec, error) {
	// Finds the column in the schema and binds it with case sensitivity.
	ref := iceberg.Reference(sourceColName)
	boundTerm, err := ref.Bind(us.txn.tbl.Schema(), us.caseSensitive)
	if err != nil {
		return nil, err
	}

	// Validate the transform
	outputType := boundTerm.Type()
	if !transform.CanTransform(outputType) {
		return nil, fmt.Errorf("{%s} cannot transform {%s} values from {%s}", transform.String(), outputType.String(), boundTerm.Ref().Field().Name)
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

	// Create the new partition field and Check for name collisions

	// Handle special case for time transforms

	// If name matches an existing field, rename it (if VOID)

	// Register the new field
}

func (us *UpdateSpec) isDuplicatePartition(transform iceberg.Transform, partitionField iceberg.PartitionField) bool {
	return false
}
