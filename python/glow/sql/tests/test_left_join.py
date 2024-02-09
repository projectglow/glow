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


def test_left_join(spark):
    left = spark.createDataFrame([("a", 1, 10), ("a", 2, 3), ("a", 5, 7), ("a", 2, 5),
                                  ("b", 1, 10)], ["name", "start", "end"])
    right = spark.createDataFrame([("a", 2, 5), ("c", 2, 5)], ["name", "start", "end"])
    joined = glow.left_range_join(left, right, left.start, right.start, left.end, right.end,
                                  left.name == right.name)
    assert joined.count() == 5
    assert joined.where(right.start.isNull()).count() == 2
    assert joined.where((right.name == "c") & (right.start == 2) & (right.end == 5)).count() == 0
