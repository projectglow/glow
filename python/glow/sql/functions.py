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

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import lit
from typeguard import typechecked

__all__ = ['left_range_join']


@typechecked
def left_range_join(left: DataFrame,
                    right: DataFrame,
                    left_start: Column,
                    right_start: Column,
                    left_end: Column,
                    right_end: Column,
                    extra_join_expr: Column = None,
                    bin_size: int = 5000) -> DataFrame:
    if extra_join_expr is None:
        extra_join_expr = lit(True)
    join_fn = SparkContext._jvm.io.projectglow.sql.LeftRangeJoin.join
    column_args = [
        _to_java_column(c) for c in [left_start, right_start, left_end, right_end, extra_join_expr]
    ]
    output_jdf = join_fn(left._jdf, right._jdf, *column_args, bin_size)
    return DataFrame(output_jdf, left.sparkSession)
