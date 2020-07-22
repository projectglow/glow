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

from glow.conversions import OneDimensionalDoubleNumpyArrayConverter, TwoDimensionalDoubleNumpyArrayConverter
from py4j import protocol
from py4j.protocol import register_input_converter
from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext, SparkSession
from typing import Any, Dict
from typeguard import check_argument_types, check_return_type


def transform(operation: str,
              df: DataFrame,
              arg_map: Dict[str, Any] = None,
              **kwargs: Any) -> DataFrame:
    """
    Apply a named transformation to a DataFrame of genomic data. All parameters apart from the input
    data and its schema are provided through the case-insensitive options map.

    There are no bounds on what a transformer may do. For instance, it's legal for a transformer
    to materialize the input DataFrame.

    Args:
        operation: Name of the operation to perform
        df: The input DataFrame
        arg_map: A string -> any map of arguments
        kwargs: Named arguments. If the arg_map is not specified, transformer args will be
          pulled from these keyword args.

    Example:
        >>> df = spark.read.format('vcf').load('test-data/1kg_sample.vcf')
        >>> piped_df = glow.transform('pipe', df, cmd=["cat"], input_formatter='vcf', output_formatter='vcf', in_vcf_header='infer')

    Returns:
        The transformed DataFrame
    """
    assert check_argument_types()

    sc = SparkContext.getOrCreate(0)
    transform_fn = SparkContext._jvm.io.projectglow.Glow.transform
    args = arg_map if arg_map is not None else kwargs
    output_jdf = transform_fn(operation, df._jdf, args)
    output_df = DataFrame(output_jdf, SQLContext.getOrCreate(sc))

    assert check_return_type(output_df)
    return output_df


def register(session: SparkSession):
    """
    Register SQL extensions and py4j converters for a Spark session.

    Args:
        session: Spark session

    Example:
        >>> import glow
        >>> glow.register(spark)
    """
    assert check_argument_types()
    session._jvm.io.projectglow.Glow.register(session._jsparkSession)


# Register input converters in idempotent fashion
glow_input_converters = [
    OneDimensionalDoubleNumpyArrayConverter, TwoDimensionalDoubleNumpyArrayConverter
]
for gic in glow_input_converters:
    if not any(type(pic) is gic for pic in protocol.INPUT_CONVERTER):
        register_input_converter(gic(), prepend=True)
