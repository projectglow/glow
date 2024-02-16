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

__all__ = ['left_overlap_join', 'left_semi_overlap_join']


def _prepare_sql_args(left: DataFrame,
                      right: DataFrame,
                      left_start: Column | None = None,
                      right_start: Column | None = None,
                      left_end: Column | None = None,
                      right_end: Column | None = None,
                      extra_join_expr: Column | None = None) -> DataFrame:
    unexpected_columns_error = ValueError(
        'Explicit start and end columns must be specified if the left and right ' +
        'DataFrames do not contain columns named start and end.')
    if left_start is None:
        if not 'start' in left.columns: raise unexpected_columns_error
        else: left_start = left.start
    if right_start is None:
        if not 'start' in right.columns: raise unexpected_columns_error
        else: right_start = right.start
    if left_end is None:
        if not 'end' in left.columns: raise unexpected_columns_error
        else: left_end = left.end
    if right_end is None:
        if not 'end' in right.columns: raise unexpected_columns_error
        else: right_end = right.end

    if extra_join_expr is None and 'contigName' in left.columns and 'contigName' in right.columns:
        extra_join_expr = left.contigName == right.contigName
    if extra_join_expr is None:
        extra_join_expr = lit(True)

    column_args = [
        _to_java_column(c) for c in [left_start, right_start, left_end, right_end, extra_join_expr]
    ]
    return [left._jdf, right._jdf] + column_args


@typechecked
def left_overlap_join(left: DataFrame,
                      right: DataFrame,
                      left_start: Column | None = None,
                      right_start: Column | None = None,
                      left_end: Column | None = None,
                      right_end: Column | None = None,
                      extra_join_expr: Column | None = None,
                      right_prefix: str | None = None,
                      bin_size: int = 5000) -> DataFrame:
    """
    Executes a left outer join with an interval overlap condition accelerated
    by `Databricks' range join optimization <https://docs.databricks.com/en/optimizations/range-join.html>`__.
    This function assumes half open intervals i.e., (0, 2) and (1, 2) overlap but (0, 2) and (2, 3) do not.

    Args:
        left: The first DataFrame to join. This DataFrame is expected to be larger and contain
          a mixture of SNPs (intervals with length 1) and longer intervals.
        right: The second DataFrame to join. It is expected to contain primarily longer intervals.
        left_start: The interval start column in the left DataFrame. It must be specified if there is not a
          column named ``start``.
        left_end: The interval end column in the left DataFrame. It must be specified if there is not a
          column named ``end``.
        right_start: The interval start column in the right DataFrame. It must be specified if there is not a
          column named ``start``.
        right_end: The interval end column in the right DataFrame. It must be specified if there is not a
          column named ``end``.
        extra_join_expr: An expression containing additional join criteria. If a column named ``contigName``
          exists in both the left and right DataFrames, the default value is ``left.contigName == right.contigName``
        right_prefix: If provided, all columns in the joined DataFrame that originated from the right DataFrame will
          have their names prefixed with this string. Can be useful if some column names are duplicated between the
          left and right DataFrames.
        bin_size: The bin size to use for the range join optimization

    Example:
        >>> left = spark.createDataFrame([(1, 10)], ["start", "end"])
        >>> right = spark.createDataFrame([(2, 3)], ["start", "end"])
        >>> df = glow.left_overlap_join(left, right)

    Returns:
        The joined DataFrame

    """
    join_fn = SparkContext._jvm.io.projectglow.sql.LeftOverlapJoin.leftJoin
    fn_args = _prepare_sql_args(left, right, left_start, right_start, left_end, right_end,
                                extra_join_expr)
    prefix_arg = SparkContext._jvm.scala.Option.apply(right_prefix)
    fn_args = fn_args + [prefix_arg, bin_size]
    output_jdf = join_fn(*fn_args)
    return DataFrame(output_jdf, left.sparkSession)


@typechecked
def left_semi_overlap_join(left: DataFrame,
                           right: DataFrame,
                           left_start: Column | None = None,
                           right_start: Column | None = None,
                           left_end: Column | None = None,
                           right_end: Column | None = None,
                           extra_join_expr: Column | None = None,
                           bin_size: int = 5000) -> DataFrame:
    """
    Executes a left semi join with an interval overlap condition accelerated
    by `Databricks' range join optimization <https://docs.databricks.com/en/optimizations/range-join.html>`__.
    This function assumes half open intervals i.e., (0, 2) and (1, 2) overlap but (0, 2) and (2, 3) do not.

    Args:
        left: The first DataFrame to join. This DataFrame is expected to be larger and contain
          a mixture of SNPs (intervals with length 1) and longer intervals.
        right: The second DataFrame to join. It is expected to contain primarily longer intervals.
        left_start: The interval start column in the left DataFrame. It must be specified if there is not a
          column named ``start``.
        left_end: The interval end column in the left DataFrame. It must be specified if there is not a
          column named ``end``.
        right_start: The interval start column in the right DataFrame. It must be specified if there is not a
          column named ``start``.
        right_end: The interval end column in the right DataFrame. It must be specified if there is not a
          column named ``end``.
        extra_join_expr: An expression containing additional join criteria. If a column named ``contigName``
          exists in both the left and right DataFrames, the default value is ``left.contigName == right.contigName``
        bin_size: The bin size to use for the range join optimization

    Example:
        >>> left = spark.createDataFrame([(1, 10)], ["start", "end"])
        >>> right = spark.createDataFrame([(2, 3)], ["start", "end"])
        >>> df = glow.left_semi_overlap_join(left, right)

    Returns:
        The joined DataFrame

    """
    join_fn = SparkContext._jvm.io.projectglow.sql.LeftOverlapJoin.leftSemiJoin
    fn_args = _prepare_sql_args(left, right, left_start, right_start, left_end, right_end,
                                extra_join_expr)
    fn_args = fn_args + [bin_size]
    output_jdf = join_fn(*fn_args)
    return DataFrame(output_jdf, left.sparkSession)
