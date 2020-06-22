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

import pytest
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
import glow as glow


def test_no_register(spark):
    sess = spark.newSession()
    row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    df = sess.createDataFrame([row_one, row_two], schema=['base_col'])
    with pytest.raises(AnalysisException):
        df.selectExpr(
            "add_struct_fields(base_col, 'float_col', 3.14, 'rev_str_col', reverse(base_col.str_col))"
        ).head()


def test_register(spark):
    sess = spark.newSession()
    glow.register(sess)
    row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    df = sess.createDataFrame([row_one, row_two], schema=['base_col'])
    added_col_row = df.selectExpr("add_struct_fields(base_col, 'float_col', 3.14, 'rev_str_col', reverse(base_col.str_col)) as added_col") \
                      .filter("added_col.str_col = 'foo'") \
                      .head()
    assert added_col_row.added_col.rev_str_col == 'oof'
