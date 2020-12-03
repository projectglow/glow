from typing import Callable, Union
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame

__all__ = ['logistic_regression']

@typechecked
def logistic_regression(genotype_df: DataFrame,
    phenotype_df: pd.DataFrame,
    covariate_df: pd.DataFrame = pd.DataFrame({}),
    offset_df: pd.DataFrame = pd.DataFrame({}),
    fallback_method: Union[str, Callable] = 'approx-firth',
    fit_intercept: bool = True,
    values_column: str = 'values'):

    