from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext
from typing import Dict
from typeguard import typechecked


@typechecked
def transform(operation: str, df: DataFrame, arg_map: Dict[str, str]=None,
              **kwargs: str) -> DataFrame:
    """
    Apply a named transformation to a DataFrame of genomic data. All parameters apart from the input
    data and its schema are provided through the case-insensitive options map.

    There are no bounds on what a transformer may do. For instance, it's legal for a transformer
    to materialize the input DataFrame.

    :param operation: Name of the operation to perform
    :param df: The input DataFrame
    :param arg_map: A string -> string map of arguments
    :param kwargs: Named string arguments. If the arg_map is not specified, transformer args will be
    pulled from these keyword args.
    :return: The transformed DataFrame
    """
    sc = SparkContext.getOrCreate(0)
    transform_fn = SparkContext._jvm.com.databricks.hls.SparkGenomics.transform
    args = arg_map if arg_map is not None else kwargs
    output_jdf = transform_fn(operation, df._jdf, args)
    return DataFrame(output_jdf, SQLContext.getOrCreate(sc))
