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

package table_test

import (
	"fmt"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"testing"
)

var testSchema = iceberg.NewSchema(1,
	iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 3, Name: "ts", Required: false, Type: iceberg.PrimitiveTypes.Timestamp},
	iceberg.NestedField{ID: 4, Name: "address", Required: false, Type: &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 6, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 7, Name: "zip_code", Type: iceberg.PrimitiveTypes.String, Required: false},
		},
	}},
)

var partitionSpec = iceberg.NewPartitionSpec(iceberg.PartitionField{SourceID: 1, FieldID: 1, Transform: iceberg.IdentityTransform{}})

var testMetadataNonPartitioned, _ = table.NewMetadata(testSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "", nil)
var testMetadataPartitioned, _ = table.NewMetadata(testSchema, &partitionSpec, table.UnsortedSortOrder, "", nil)

var testNonPartitionedTable = table.New([]string{"non_partitioned"}, testMetadataNonPartitioned, "", nil, nil)
var testPartitionedTable = table.New([]string{"partitioned"}, testMetadataPartitioned, "", nil, nil)

func TestUpdateSpecAddField(t *testing.T) {
	txn := testPartitionedTable.NewTransaction()
	updates := table.NewUpdateSpec(txn, false)

	t.Run("add new patition field with year transform", func(t *testing.T) {
		updated, err := updates.AddField("ts", iceberg.YearTransform{}, "year_transform")
		assert.NoError(t, err)
		assert.NotNil(t, updated)

		newSpec := updated.Apply()
		fmt.Printf("applied new spec: %s\n", newSpec.String())
		//assert.Equal(t, newSpec.ID, 0)
		//assert.Equal(t, newSpec.LastAssignedFieldID(), 0)
		//assert.Equal(t, newSpec.NumFields, 0)
		//for field := range newSpec.Fields() {
		//	assert.Equal(t, nil, field)
		//}
	})
}

//func validatePartitionSpec(t *testing.T, expected *iceberg.PartitionSpec, actual *iceberg.PartitionSpec) {
//	assert.NotNil(t, expected)
//	assert.NotNil(t, actual)
//}
