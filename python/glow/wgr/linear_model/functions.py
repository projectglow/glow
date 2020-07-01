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

import itertools
import math
from nptyping import Float, Int, NDArray
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import re
from typeguard import typechecked
from typing import Any, Dict, Iterable, List, Tuple
import warnings


@typechecked
def sort_in_place(pdf: pd.DataFrame, columns: List[str]) -> None:
    """
    A faster alternative to DataFrame.sort_values. Note that this function is less sophisticated
    than sort_values and does not allow for control over sort direction or null handling.

    Adapted from https://github.com/pandas-dev/pandas/issues/15389.

    Args:
        pdf : The pandas DataFrame to sort
        columns : Columns to sort by
    """
    order = np.lexsort([pdf[col].array for col in reversed(columns)])
    for col in list(pdf.columns):
        pdf[col].array[:] = pdf[col].array[order]


@typechecked
def parse_key(key: Tuple, key_pattern: List[str]) -> Tuple[str, str, str]:
    """
    Interprets the key corresponding to a group from a groupBy clause.  The key may be of the form:
        (header_block, sample_block),
        (header_block, sample_block, label),
        (header_block, header),
        (header_block, header, label),
        (sample_block, label)
    depending on the context.  In each case, a tuple with 3 members is returned, with the missing member filled in by
    'all' where necessary

    Args:
        key : key for the group
        key_pattern : one of the aforementioned key patterns

    Returns:
        tuple of (header_block, sample_block, label) or (header_block, header, label), where header_block or label may be filled with 'all'
        depending on context.
    """
    if key_pattern == ['header_block', 'sample_block']:
        return key[0], key[1], 'all'
    elif key_pattern == ['header_block', 'header']:
        return key[0], key[1], 'all'
    elif key_pattern == ['sample_block', 'label']:
        return 'all', key[0], key[1]
    elif len(key) != 3:
        raise ValueError(f'Key must have 3 values, pattern is {key_pattern}')
    else:
        return key


@typechecked
def assemble_block(n_rows: Int, n_cols: Int, pdf: pd.DataFrame,
                   cov_matrix: NDArray[(Any, Any), Float]) -> NDArray[Float]:
    """
    Creates a dense n_rows by n_cols matrix from the array of either sparse or dense vectors in the Pandas DataFrame
    corresponding to a group.  This matrix represents a block.

    Args:
         n_rows : The number of rows in the resulting matrix
         n_cols : The number of columns in the resulting matrix
         pdf : Pandas DataFrame corresponding to a group
         cov_matrix: 2D numpy array representing covariate columns that should be prepended to matrix X from the block.  Can be
            empty if covariates are not being applied.

    Returns:
        Dense n_rows by n_columns matrix where the columns have been 0-centered and standard scaled.
    """
    mu = pdf['mu'].to_numpy()
    sig = pdf['sig'].to_numpy()

    if 0 in sig:
        raise ValueError(f'Standard deviation cannot be 0.')

    if 'indices' not in pdf.columns:
        X_raw = np.column_stack(pdf['values'].array)
    else:
        X_raw = np.zeros([n_rows, n_cols])
        for column, row in enumerate(pdf[['indices', 'values']].itertuples()):
            X_raw[row.indices, column] = row.values

    X = ((X_raw - mu) / sig)

    if cov_matrix.any():
        return np.column_stack((cov_matrix, X))
    else:
        return X


@typechecked
def slice_label_rows(labeldf: pd.DataFrame, label: str, sample_list: List[str]) -> NDArray[Float]:
    """
    Selects rows from the Pandas DataFrame of labels corresponding to the samples in a particular sample_block.

    Args:
        labeldf : Pandas DataFrame containing the labels
        label : Header for the particular label to slice.  Can be 'all' if all labels are desired.
        sample_list : List of sample ids corresponding to the sample_block to be sliced out.

    Returns:
        Matrix of [number of samples in sample_block] x [number of labels to slice]
    """
    if label == 'all':
        return labeldf.loc[sample_list, :].to_numpy()
    else:
        return labeldf[label].loc[sample_list].to_numpy().reshape(-1, 1)


@typechecked
def evaluate_coefficients(pdf: pd.DataFrame, alpha_values: Iterable[Float],
                          n_cov: int) -> NDArray[Float]:
    """
    Solves the system (XTX + Ia)^-1 * XtY for each of the a values in alphas.  Returns the resulting coefficients.

    Args:
         pdf : Pandas DataFrame for the group
         alpha_values : Array of alpha values (regularization strengths)
         n_cov: Number of covariate columns on the left-most side of matrix X.  These are regularized with a constant
         value of alpha = 1, regardless of the alpha value being used for the rest of the matrix X.

    Returns:
        Matrix of coefficients of size [number of columns in X] x [number of labels * number of alpha values]
    """
    XtX = np.stack(pdf['xtx'].array)
    XtY = np.stack(pdf['xty'].array)
    diags = [
        np.concatenate([np.ones(n_cov), np.ones(XtX.shape[1] - n_cov) * a]) for a in alpha_values
    ]
    return np.column_stack([(np.linalg.inv(XtX + np.diag(d)) @ XtY) for d in diags])


@typechecked
def cross_alphas_and_labels(alpha_names: Iterable[str], labeldf: pd.DataFrame,
                            label: str) -> List[Tuple[str, str]]:
    """
    Crosses all label and alpha names. The output tuples appear in the same order as the output of
    evaluate_coefficients.

    Args:
        alpha_names : List of string identifiers assigned to the values of alpha
        labeldf : Pandas DataFrame of labels
        label : Label used for this set of coefficients.  Can be 'all' if all labels were used.
    Returns:
        List of tuples of the form (alpha_name, label_name)
    """
    if label == 'all':
        label_names = labeldf.columns
    else:
        label_names = [label]

    return list(itertools.product(alpha_names, label_names))


@typechecked
def new_headers(header_block: str, alpha_names: Iterable[str],
                row_indexer: List[Tuple[str, str]]) -> Tuple[str, List[int], List[str]]:
    """
    Creates new headers for the output of a matrix reduction step.  Generally produces names like
    "block_[header_block_number]_alpha_[alpha_name]_label_[label_name]"

    Args:
        header_block : Identifier for a header_block (e.g., 'chr_1_block_0')
        alpha_names : List of string identifiers for alpha parameters
        row_indexer : A list of tuples provided by the cross_alphas_and_labels function

    Returns:
        new_header_block : A new header_block name, typically the chromosome (e.g. chr_1), but might be 'all' if
        there are no more levels to reduce over.
        sort_keys : Array of sortable integers to specify the ordering of the new matrix headers.
        headers : List of new matrix headers.
    """
    match_chr_block = re.search(r"^chr_(.*)_block_([0-9]+)$", header_block)
    match_chr = re.search(r"^chr_(.*)$", header_block)
    if match_chr_block:
        chr = match_chr_block.group(1)
        inner_index = int(match_chr_block.group(2))
        header_prefix = f'chr_{chr}_block_{inner_index}_'
        new_header_block = f'chr_{chr}'
    elif match_chr:
        chr = match_chr.group(1)
        inner_index = abs(hash(chr)) % (10**8)  # Hash to 8 digits
        header_prefix = f'chr_{chr}_'
        new_header_block = 'all'
    elif header_block == 'all':
        inner_index = 0
        header_prefix = ''
        new_header_block = 'all'
    else:
        raise ValueError(f'Header block {header_block} does not match expected pattern.')

    sort_keys, headers = [], []
    for a, l in row_indexer:
        sort_key = inner_index * len(alpha_names) + int(re.search(r"^alpha_([0-9]+)", a).group(1))
        sort_keys.append(sort_key)
        header = f'{header_prefix}{a}_label_{l}'
        headers.append(header)

    return new_header_block, sort_keys, headers


@typechecked
def r_squared(XB: NDArray[Float], Y: NDArray[Float]) -> NDArray[(Any, ), Float]:
    """
    Computes the coefficient of determination (R2) metric between the matrix resulting from X*B and the matrix of labels
    Y.

    Args:
        XB : Matrix representing the result of the multiplication X*B, where X is a matrix of [number of samples] x
        [number of headers] and B is a matrix of [number of headers x number of alphas * number of labels]
        Y : Matrix of labels, with [number of samples x number of labels]

    Returns:
        Array of [number of alphas * number of labels]
    """
    tot = np.power(Y - Y.mean(), 2).sum()
    res = np.power(Y - XB, 2).sum(axis=0)
    return 1 - (res / tot)


@typechecked
def create_alpha_dict(alphas: NDArray[(Any, ), Float]) -> Dict[str, Float]:
    """
    Creates a mapping to attach string identifiers to alpha values.

    Args:
        alphas : Alpha values

    Returns:
        Dict of [alpha names, alpha values]
    """
    return {f'alpha_{i}': a for i, a in enumerate(alphas)}


@typechecked
def generate_alphas(blockdf: DataFrame) -> Dict[str, Float]:
    """
    Generates alpha values using a range of heritability values and the number of headers.

    Args:
        blockdf : Spark DataFrame representing a block matrix
        labeldf: Pandas DataFrame containing target labels

    Returns:
        Dict of [alpha names, alpha values]
    """
    num_headers = blockdf.select('header').distinct().count()
    heritability_vals = [0.99, 0.75, 0.50, 0.25, 0.01]
    alphas = np.array([num_headers / h for h in heritability_vals])
    print(f"Generated alphas: {alphas}")
    return create_alpha_dict(alphas)


@typechecked
def __assert_all_present(df: pd.DataFrame, name: str) -> None:
    """
    Raises an error if a pandas DataFrame has missing values.

    Args:
        df : Pandas DataFrame
    """
    for label, isnull in df.isnull().any().items():
        if isnull:
            raise ValueError(f"Missing values are present in the {name} dataframe's {label} column")


@typechecked
def __check_standardized(df: pd.DataFrame, name: str) -> None:
    """
    Warns if any column of a pandas DataFrame is not standardized to zero mean and unit (biased) standard deviation.

    Args:
        df : Pandas DataFrame
    """
    for label, mean in df.mean().items():
        if not math.isclose(mean, 0, abs_tol=1e-9):
            warnings.warn(f"Mean for the {name} dataframe's column {label} should be 0, is {mean}",
                          UserWarning)
    for label, std in df.std(ddof=0).items():
        if not math.isclose(std, 1, abs_tol=0.01):
            warnings.warn(
                f"Standard deviation for the {name} dataframe's column {label} should be approximately 1, is {std}",
                UserWarning)


@typechecked
def validate_inputs(labeldf: pd.DataFrame, covdf: pd.DataFrame) -> None:
    """
    Performs basic input validation on the label and covariates pandas DataFrames. The label DataFrame cannot have
    missing values, and should be standardized to zero mean and unit standard deviation. The covariates DataFrame
    cannot have missing values.

    Args:
        labeldf : Pandas DataFrame containing target labels
        covdf : Pandas DataFrame containing covariates
    """
    __assert_all_present(labeldf, 'label')
    __assert_all_present(covdf, 'covariate')
    __check_standardized(labeldf, 'label')
    __check_standardized(covdf, 'covariate')


@typechecked
def infer_chromosomes(blockdf: DataFrame) -> List[str]:
    """
    Extracts chromosomes from a once- or twice-reduced block DataFrame.

    Args:
        blockdf : Spark DataFrame representing a once- or twice-reduced block matrix.

    Returns:
        List of chromosomes.
    """
    # Regex captures the chromosome name in the header
    # level 1 header: chr_3_block_8_alpha_0_label_sim100
    # level 2 header: chr_3_alpha_0_label_sim100
    chromosomes = [
        r.chromosome for r in blockdf.select(
            f.regexp_extract('header', r"^chr_(.+?)_(alpha|block)", 1).alias(
                'chromosome')).distinct().collect()
    ]
    print(f'Inferred chromosomes: {chromosomes}')
    return chromosomes
