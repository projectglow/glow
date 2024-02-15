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
import pytest


def test_left_join(spark):
    left = spark.createDataFrame([("a", 1, 10), ("a", 2, 3), ("a", 5, 7), ("a", 2, 5),
                                  ("b", 1, 10)], ["name", "start", "end"])
    right = spark.createDataFrame([("a", 2, 5), ("c", 2, 5)], ["name", "start", "end"])
    joined = glow.left_overlap_join(left, right, left.start, right.start, left.end, right.end,
                                    left.name == right.name)
    assert joined.count() == 5
    assert joined.where(right.start.isNull()).count() == 2
    assert joined.where((right.name == "c") & (right.start == 2) & (right.end == 5)).count() == 0


def test_left_semi_join(spark):
    left = spark.createDataFrame([("a", 1, 10), ("a", 2, 3), ("a", 5, 7), ("a", 2, 5),
                                  ("b", 1, 10)], ["name", "start", "end"])
    right = spark.createDataFrame([("a", 2, 5), ("c", 1, 10)], ["name", "start", "end"])
    joined = glow.left_semi_overlap_join(left, right, left.start, right.start, left.end, right.end,
                                         left.name == right.name)
    assert joined.count() == 3


@pytest.mark.parametrize('join_fn', [glow.left_overlap_join, glow.left_semi_overlap_join])
def test_no_extra_expr(spark, join_fn):
    left = spark.createDataFrame([(1, 10)], ["start", "end"])
    right = spark.createDataFrame([(1, 10)], ["start", "end"])
    joined = join_fn(left, right, left.start, right.start, left.end, right.end)
    assert joined.count() == 1


@pytest.mark.parametrize('join_fn', [glow.left_overlap_join, glow.left_semi_overlap_join])
def test_bin_size(spark, join_fn):
    left = spark.createDataFrame([(1, 10)], ["start", "end"])
    right = spark.createDataFrame([(1, 10)], ["start", "end"])
    joined = join_fn(left, right, left.start, right.start, left.end, right.end, bin_size=1)
    assert joined.count() == 1


def test_default_arguments(spark):
    left = spark.createDataFrame([("a", 1, 10), ("a", 2, 3), ("a", 5, 7), ("a", 2, 5),
                                  ("b", 1, 10)], ["contigName", "start", "end"])
    right = spark.createDataFrame([("a", 2, 5), ("c", 2, 5)], ["contigName", "start", "end"])
    joined = glow.left_overlap_join(left, right)
    assert joined.count() == 5
    assert joined.where(right.start.isNull()).count() == 2
    assert joined.where((right.contigName == "c") & (right.start == 2) &
                        (right.end == 5)).count() == 0


@pytest.mark.parametrize('join_fn', [glow.left_overlap_join, glow.left_semi_overlap_join])
def test_default_arguments_no_contig(spark, join_fn):
    left = spark.createDataFrame([(1, 10)], ["start", "end"])
    right = spark.createDataFrame([(1, 10)], ["start", "end"])
    assert join_fn(left, right).count() == 1


@pytest.mark.parametrize('join_fn', [glow.left_overlap_join, glow.left_semi_overlap_join])
def test_missing_columns(spark, join_fn):
    left = spark.createDataFrame([(1, 10)], ["start", "end"])
    right = spark.createDataFrame([(1, 10)], ["start", "end"])
    args = {
        'left_start': left.start,
        'right_start': right.start,
        'left_end': left.end,
        'right_end': right.end
    }
    join_fn(left, right, **args)  # No error
    for k in args.keys():
        d = args.copy()
        d.pop(k)
        l = left.drop(args[k]) if 'left' in k else left
        r = right.drop(args[k]) if 'right' in k else right
        with pytest.raises(ValueError):
            join_fn(l, r, **d)


def test_right_prefix(spark):
    left = spark.createDataFrame([("a", 1, 10), ("a", 2, 3), ("a", 5, 7), ("a", 2, 5),
                                  ("b", 1, 10)], ["name", "start", "end"])
    right = spark.createDataFrame([("a", 2, 5, 'dog'), ("c", 2, 5, 'cat')],
                                  ["name", "start", "end", "animal"])
    joined = glow.left_overlap_join(left, right, left.start, right.start, left.end, right.end,
                                    left.name == right.name, 'ann')
    assert all([c in joined.columns for c in ['ann_name', 'ann_start', 'ann_end', 'ann_animal']])
    assert joined.count() == 5
