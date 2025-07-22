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

import argparse
import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

def testSetProperties():
    df = spark.sql("SHOW TBLPROPERTIES default.go_test_set_properties")
    props = {row['key']: row['value'] for row in df.collect()}
    df.show(truncate=False)

    assert props["write.parquet.compression-codec"] == "snappy"
    assert props["commit.manifest-merge.enabled"] == "true"
    assert props["commit.manifest.min-count-to-merge"] == "1"
    assert props["commit.manifest.target-size-bytes"] == "1"


def testAddedFile():
    df = spark.sql("SELECT COUNT(*) FROM default.test_partitioned_by_days")
    rows = df.collect()
    df.show(truncate=False)

    assert len(rows) == 1
    assert rows[0]["count(1)"] == 13


def testReadDifferentDataTypes():
    actual_df = spark.sql("SELECT * FROM default.go_test_different_data_types")
    actual_df.show(truncate=False)
    actual_rows = actual_df.collect()

    schema = StructType([
        StructField("bool", BooleanType(), True),
        StructField("string", StringType(), True),
        StructField("string_long", StringType(), True),
        StructField("int", IntegerType(), True),
        StructField("long", LongType(), True),
        StructField("float", FloatType(), True),
        StructField("double", DoubleType(), True),
        StructField("timestamp", TimestampNTZType(), True),
        StructField("timestamptz", TimestampType(), True),
        StructField("date", DateType(), True),
        StructField("uuid", StringType(), True),
        StructField("binary", BinaryType(), True),
        StructField("fixed", BinaryType(), True),
        StructField("small_dec", DecimalType(8, 2), True),
        StructField("med_dec", DecimalType(16, 2), True),
        StructField("large_dec", DecimalType(24, 2), True),
        StructField("list", ArrayType(IntegerType()), True),
    ])

    expected = [
        Row(
            bool=False,
            string="a",
            string_long="a" * 22,
            int=1,
            long=1,
            float=0.0,
            double=0.0,
            timestamp=datetime.datetime(2023, 1, 1, 11, 25),
            timestamptz=datetime.datetime(2023, 1, 1, 19, 25),
            date=datetime.date(2023, 1, 1),
            uuid="00000000-0000-0000-0000-000000000000",
            binary=bytearray(b"\x01"),
            fixed=bytearray(b"\x00" * 16),
            small_dec=Decimal("123456.78"),
            med_dec=Decimal("12345678901234.56"),
            large_dec=Decimal("1234567890123456789012.34"),
            list=[1, 2, 3],
        ),
        Row(
            bool=None,
            string=None,
            string_long=None,
            int=None,
            long=None,
            float=None,
            double=None,
            timestamp=None,
            timestamptz=None,
            date=None,
            uuid=None,
            binary=None,
            fixed=None,
            small_dec=None,
            med_dec=None,
            large_dec=None,
            list=None,
        ),
        Row(
            bool=True,
            string="z",
            string_long="z" * 22,
            int=9,
            long=9,
            float=0.9,
            double=0.9,
            timestamp=datetime.datetime(2023, 3, 1, 11, 25),
            timestamptz=datetime.datetime(2023, 3, 1, 19, 25),
            date=datetime.date(2023, 3, 1),
            uuid="11111111-1111-1111-1111-111111111111",
            binary=bytearray(b"\x12"),
            fixed=bytearray(b"\x11" * 16),
            small_dec=Decimal("876543.21"),
            med_dec=Decimal("65432109876543.21"),
            large_dec=Decimal("4321098765432109876543.21"),
            list=[-1, -2, -3],
        ),
    ]

    expected_df = spark.createDataFrame(expected, schema=schema)
    assert expected_df.schema == actual_df.schema, "Schema mismatch"

    expected_rows = expected_df.collect()
    assert len(expected_rows) == len(actual_rows)
    assert expected_rows == actual_rows


def testReadSpecUpdate():
    df = spark.sql("DESCRIBE TABLE EXTENDED default.go_test_update_spec")
    df.show(truncate=False)

    rows = df.collect()

    metadata = {row["col_name"]: row["data_type"] for row in rows if row["col_name"] is not None}

    assert metadata["Part 0"] == "truncate(5, bar)"
    assert metadata["Part 1"] == "bucket(3, baz)"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", type=str, required=True, help="Name of the test to run")
    args = parser.parse_args()

    if args.test == "TestSetProperties":
        testSetProperties()

    if args.test == "TestAddedFile":
        testAddedFile()

    if args.test == "TestReadDifferentDataTypes":
        testReadDifferentDataTypes()

    if args.test == "TestReadSpecUpdate":
        testReadSpecUpdate()
