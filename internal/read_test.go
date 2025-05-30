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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

type ReadTestSuite struct {
	suite.Suite

	ctx   context.Context
	cat   catalog.Catalog
	props iceberg.Properties
}

func (s *ReadTestSuite) SetupSuite() {
	//_, err := recipe.Start(s.T())
	//require.NoError(s.T(), err)
}

func (s *ReadTestSuite) SetupTest() {
	s.ctx = context.Background()

	cat, err := rest.NewCatalog(s.ctx, "rest", "http://localhost:8181")
	s.Require().NoError(err)

	s.cat = cat
	s.props = iceberg.Properties{
		io.S3Region:      "us-east-1",
		io.S3AccessKeyID: "admin", io.S3SecretAccessKey: "password",
	}
}

func (s *ReadTestSuite) TestRead() {
	tests := []struct {
		table           string
		expr            iceberg.BooleanExpression
		expectedNumRows int
	}{
		{"default.test_uuid_and_fixed_unpartitioned", iceberg.AlwaysTrue{}, 5},
		{"default.test_null_nan", iceberg.AlwaysTrue{}, 3},
	}

	for _, tt := range tests {
		s.Run(tt.table+" "+tt.expr.String(), func() {
			ident := catalog.ToIdentifier(tt.table)

			tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
			s.Require().NoError(err)

			arrTbl, err := tbl.Scan(table.WithRowFilter(tt.expr)).ToArrowTable(s.ctx)
			s.Require().NoError(err)

			s.Equal(tt.expectedNumRows, int(arrTbl.NumRows()))
		})
	}
}

func TestRead(t *testing.T) {
	suite.Run(t, new(ReadTestSuite))
}
