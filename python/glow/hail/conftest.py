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

import hail as hl
import pytest


# Override Spark session for Hail tests
@pytest.fixture(autouse=True, scope="module")
def spark(spark_builder):
    print("set up new spark session with Hail configs")
    sess = spark_builder \
        .config("spark.jars", "hail/hail/build/libs/hail-all-spark.jar") \
        .config("spark.driver.extraClassPath", "hail/hail/build/libs/hail-all-spark.jar") \
        .config("spark.executor.extraClassPath", "./hail-all-spark.jar") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator") \
        .getOrCreate() \
        .newSession()
    hl.init(sess.sparkContext, idempotent=True, quiet=True)
    return sess
