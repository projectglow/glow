from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext, SparkSession
from typing import Dict
from typeguard import check_argument_types, check_return_type


def transform(operation: str, df: DataFrame, arg_map: Dict[str, str]=None,
              **kwargs: str) -> DataFrame:
    """
    transform(operation, df, arg_map = None, **kwargs) -> DataFrame

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
    register(session)

    Register SQL extensions for a Spark session.

    :param session: Spark session
    """
    assert check_argument_types()
    session._jvm.io.projectglow.sql.SqlExtensionProvider.register(session._jsparkSession)
