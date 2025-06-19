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

type UpdateSpec struct {
	txn *Transaction
}

func NewUpdateSpec(t *Transaction) *UpdateSpec {
	return &UpdateSpec{
		txn: t,
	}
}

func (us *UpdateSpec) AddField() {
	// Finds the column in the schema and binds it with case sensitivity.

	// Validate the transform

	// Check for duplicate transform on same column

	// Check if this transform was already added

	// Create the new partition field and Check for name collisions

	// Handle special case for time transforms

	// If name matches an existing field, rename it (if VOID)

	// Register the new field
}
