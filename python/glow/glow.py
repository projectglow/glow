from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext, SparkSession
from typing import Dict
from typeguard import check_argument_types, check_return_type


def transform(operation: str, df: DataFrame, arg_map: Dict[str, str]=None,
              **kwargs: str) -> DataFrame:
    """
    Apply a named transformation to a DataFrame of genomic data. All parameters apart from the input
    data and its schema are provided through the case-insensitive options map.

    There are no bounds on what a transformer may do. For instance, it's legal for a transformer
    to materialize the input DataFrame.

    Args:
        operation: Name of the operation to perform
        df: The input DataFrame
        arg_map: A string -> string map of arguments
        kwargs: Named string arguments. If the arg_map is not specified, transformer args will be
          pulled from these keyword args.

    Example:
        >>> df = spark.read.format('vcf').load('test-data/1kg_sample.vcf')
        >>> piped_df = glow.transform('pipe', df, cmd='["cat"]', input_formatter='vcf', output_formatter='vcf', in_vcf_header='infer')

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
    Register SQL extensions for a Spark session.

    Args:
        session: Spark session

    Example:
        >>> import glow
        >>> glow.register(spark)
    """
    assert check_argument_types()
    session._jvm.io.projectglow.Glow.register(session._jsparkSession)
