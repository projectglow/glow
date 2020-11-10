# Copyright 2019 The Glow Authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="module")
def spark_builder():
    return SparkSession.builder \
        .master("local[2]") \
        .config("spark.hadoop.io.compression.codecs", "io.projectglow.sql.util.BGZFCodec") \
        .config("spark.ui.enabled", "false")


# Set up a new Spark session for each test suite
@pytest.fixture(scope="module")
def spark(spark_builder):
    print("set up new spark session")
    sess = spark_builder.getOrCreate()
    return sess.newSession()
