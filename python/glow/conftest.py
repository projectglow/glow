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

import glow
from pyspark.sql import functions, Row
import pytest


@pytest.fixture(scope="module", autouse=True)
def spark(doctest_namespace, raw_spark):
    spark = glow.register(raw_spark)
    doctest_namespace['Row'] = Row
    doctest_namespace['spark'] = spark
    doctest_namespace['lit'] = functions.lit
    doctest_namespace['col'] = functions.col
    doctest_namespace['glow'] = glow
    return spark