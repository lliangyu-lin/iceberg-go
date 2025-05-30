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

//go:build integration

package internal_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

type WriteTestSuite struct {
	suite.Suite

	ctx   context.Context
	cat   catalog.Catalog
	props iceberg.Properties
}

func (s *WriteTestSuite) SetupSuite() {
	//_, err := recipe.Start(s.T())
	//require.NoError(s.T(), err)
}

func (s *WriteTestSuite) SetupTest() {
	s.ctx = context.Background()

	cat, err := rest.NewCatalog(s.ctx, "rest", "http://localhost:8181")
	s.Require().NoError(err)

	s.cat = cat
	s.props = iceberg.Properties{
		io.S3EndpointURL: "http://minio:9000",
		io.S3Region:      "us-east-1",
		io.S3AccessKeyID: "admin", io.S3SecretAccessKey: "password",
	}
}

func (s *WriteTestSuite) TestWrite() {
	//nsIdent := catalog.ToIdentifier("default")
	//err := s.cat.CreateNamespace(s.ctx, nsIdent, s.props)
	//s.Require().NoError(err)

	testTableIdent := catalog.ToIdentifier("write_database", "table1")

	testSchema := iceberg.NewSchemaWithIdentifiers(0, []int{},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool})

	testTable, err := s.cat.CreateTable(s.ctx, testTableIdent, testSchema,
		catalog.WithProperties(iceberg.Properties{}))
	s.Require().NoError(err)

	sc, err := table.SchemaToArrowSchema(testSchema, nil, true, false)
	if err != nil {
		panic(err)
	}

	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, sc, []string{
		`[
			{
				"foo": "a",
				"bar": 1,
				"baz": false
			},
			{
				"foo": "b",
				"bar": 2,
				"baz": true
			}
		 ]`,
	})
	if err != nil {
		panic(err)
	}

	defer arrTable.Release()

	testTable, err = testTable.AppendTable(s.ctx, arrTable, 1, nil)
	s.Require().NoError(err)

	//arrTbl, err := testTable.Scan(table.WithRowFilter(iceberg.AlwaysTrue{})).ToArrowTable(s.ctx)
	//s.Require().NoError(err)
	//
	//s.Equal(2, int(arrTbl.NumRows()))
}

func TestWrite(t *testing.T) {
	suite.Run(t, new(WriteTestSuite))
}
