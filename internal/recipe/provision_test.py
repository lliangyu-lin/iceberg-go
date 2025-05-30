# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import FixedType, NestedField, UUIDType

spark = SparkSession.builder.getOrCreate()

catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": "http://rest:8181",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )

spark.sql("CREATE DATABASE IF NOT EXISTS default;")

schema = Schema(
        NestedField(field_id=1, name="uuid_col", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="fixed_col", field_type=FixedType(25), required=False),
    )

catalog.create_table(identifier=f"default.test_uuid_and_fixed_unpartitioned", schema=schema)

spark.sql(
    f"""
    INSERT INTO default.test_uuid_and_fixed_unpartitioned VALUES
    ('102cb62f-e6f8-4eb0-9973-d9b012ff0967', CAST('1234567890123456789012345' AS BINARY)),
    ('ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226', CAST('1231231231231231231231231' AS BINARY)),
    ('639cccce-c9d2-494a-a78c-278ab234f024', CAST('12345678901234567ass12345' AS BINARY)),
    ('c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b', CAST('asdasasdads12312312312111' AS BINARY)),
    ('923dae77-83d6-47cd-b4b0-d383e64ee57e', CAST('qweeqwwqq1231231231231111' AS BINARY));
    """
)

spark.sql(
f"""
  CREATE OR REPLACE TABLE default.test_null_nan
  USING iceberg
  AS SELECT
    1            AS idx,
    float('NaN') AS col_numeric
UNION ALL SELECT
    2            AS idx,
    null         AS col_numeric
UNION ALL SELECT
    3            AS idx,
    1            AS col_numeric;
"""
)
