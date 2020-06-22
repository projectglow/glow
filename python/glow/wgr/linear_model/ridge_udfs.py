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

from .functions import *
from nptyping import Float
import pandas as pd
from pyspark.sql.types import ArrayType, IntegerType, FloatType, StructType, StructField, StringType, DoubleType
from typeguard import typechecked
from typing import Dict, List
'''
Each function in this module performs a Pandas DataFrame => Pandas DataFrame transformation, and each is intended to be
used as a Pandas GROUPED_MAP UDF.
'''
normal_eqn_struct = StructType([
    StructField('header_block', StringType()),
    StructField('sample_block', StringType()),
    StructField('label', StringType()),
    StructField('header', StringType()),
    StructField('sort_key', IntegerType()),
    StructField('xtx', ArrayType(DoubleType())),
    StructField('xty', ArrayType(DoubleType()))
])

model_struct = StructType([
    StructField('header_block', StringType()),
    StructField('sample_block', StringType()),
    StructField('header', StringType()),
    StructField('sort_key', IntegerType()),
    StructField('alphas', ArrayType(StringType())),
    StructField('labels', ArrayType(StringType())),
    StructField('coefficients', ArrayType(DoubleType()))
])

reduced_matrix_struct = StructType([
    StructField('header', StringType()),
    StructField('size', IntegerType()),
    StructField('values', ArrayType(DoubleType())),
    StructField('header_block', StringType()),
    StructField('sample_block', StringType()),
    StructField('sort_key', IntegerType()),
    StructField('mu', DoubleType()),
    StructField('sig', DoubleType()),
    StructField('alpha', StringType()),
    StructField('label', StringType())
])

cv_struct = StructType([
    StructField('sample_block', StringType()),
    StructField('label', StringType()),
    StructField('alpha', StringType()),
    StructField('r2', DoubleType())
])


@typechecked
def map_normal_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                   sample_index: Dict[str, List[str]], covdf: pd.DataFrame) -> pd.DataFrame:
    """
    This function constructs matrices X and Y, and returns X_transpose * X (XtX) and X_transpose * Y (XtY), where X
    corresponds to a block from a block matrix.

    Each block X is uniquely identified by a header_block ID, which maps to a set of contiguous columns in the overall block
    matrix, and a sample_block ID, which maps to a set of rows in the overall block matrix (and likewise a set of rows in the
    label matrix Y).  The key that identifies X is therefore of the form (header_blockID, sample_blockID).  In some contexts, the
    block matrix will be tied to a particular label from Y, in which case the key will be of the form
    (header_block, sample_block, label).

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement.
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame used to build X and Y for block X identified by :key:.
            schema:
             |-- header: string
             |-- size: integer
             |-- indices: array (Required only if the matrix is sparse)
             |    |-- element: integer
             |-- values: array
             |    |-- element: double
             |-- header_block: string
             |-- sample_block: string
             |-- sort_key: integer
             |-- mu: double
             |-- sig: double
             |-- alpha: double (Required only if the header is tied to a specific value of alpha)
             |-- label: double (Required only if the header is tied to a specific label)
        labeldf : Pandas DataFrame containing label values (i. e., the Y in the normal equation above).
        sample_index : sample_index: dict containing a mapping of sample_block ID to a list of corresponding sample IDs
        covdf: Pandas DataFrame containing covariates that should be included with every block X above (can be empty).

    Returns:
        transformed Pandas DataFrame containing XtX and XtY corresponding to a particular block X.
            schema (specified by the normal_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- header: string
             |-- sort_key: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
    """
    header_block, sample_block, label = parse_key(key, key_pattern)
    sort_in_place(pdf, ['sort_key', 'header'])
    n_rows = pdf['size'][0]
    n_cols = len(pdf)
    sample_list = sample_index[sample_block]

    if covdf.empty:
        header_col = pdf['header']
        sort_key_col = pdf['sort_key']
        X = assemble_block(n_rows, n_cols, pdf, np.array([]))
    else:
        cov_matrix = slice_label_rows(covdf, 'all', sample_list)
        n_cov = len(covdf.columns)
        header_col = np.concatenate([covdf.columns, pdf['header']])
        #Add new sort_keys for covariates, starting from -n_cov up to 0 to ensure they come ahead of the headers.
        sort_key_col = np.concatenate((np.arange(-n_cov, 0), pdf['sort_key']))
        X = assemble_block(n_rows, n_cols, pdf, cov_matrix)

    Y = slice_label_rows(labeldf, label, sample_list)
    XtX = X.T @ X
    XtY = X.T @ Y

    data = {
        'header_block': header_block,
        'sample_block': sample_block,
        'label': label,
        'header': header_col,
        'sort_key': sort_key_col,
        'xtx': list(XtX),
        'xty': list(XtY)
    }

    return pd.DataFrame(data)


@typechecked
def reduce_normal_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame):
    """
    This function constructs lists of rows from the XtX and XtY matrices corresponding to a particular header in X but
    evaluated in different sample_blocks, and then reduces those lists by element-wise summation.  This reduction is
    repeated once for each sample_block, where the contribution of that sample_block is omitted.  There is therefore a
    one-to-one mapping of the starting lists and the reduced lists, e.g.:

        Input:
            List(xtx_sample_block0, xtx_sample_block1, ..., xtx_sample_blockN)
        Output:
            List(xtx_sum_excluding_sample_block0, xtx_sum_excluding_sample_block1, ..., xtx_sum_excluding_sample_blockN)

    Args:
        key : unique key identifying the rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement
        pdf : starting Pandas DataFrame containing the lists of rows from XtX and XtY for block X identified by :key:
            schema (specified by the normal_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- header: string
             |-- sort_key: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double

    Returns:
        transformed Pandas DataFrame containing the aggregated leave-fold-out rows from XtX and XtY
            schema (specified by the normal_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- header: string
             |-- sort_key: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
    """
    sum_xtx = pdf['xtx'].sum()
    sum_xty = pdf['xty'].sum()

    # Use numpy broadcast to subtract each row from the sum
    pdf['xtx'] = list(sum_xtx - np.vstack(pdf['xtx'].array))
    pdf['xty'] = list(sum_xty - np.vstack(pdf['xty'].array))

    return pdf


@typechecked
def solve_normal_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                     alphas: Dict[str, Float], covdf: pd.DataFrame) -> pd.DataFrame:
    """
    This function assembles the matrices XtX and XtY for a particular sample_block (where the contribution of that sample_block
    has been omitted) and solves the equation [(XtX + I*alpha)]-1 * XtY = B for a list of alpha values, and returns the
    coefficient matrix B, where B has 1 row per header in the block X and 1 column per combination of alpha value and
    label.

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame containing the lists of rows from XtX and XtY for block X identified by :key:
            schema (specified by the normal_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- header: string
             |-- sort_key: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
        labeldf : Pandas DataFrame containing label values (i. e., the Y in the normal equation above).
        alphas : dict of {alphaName : alphaValue} for the alpha values to be used
        covdf: Pandas DataFrame containing covariates that should be included with every block X above (can be empty).

    Returns:
        transformed Pandas DataFrame containing the coefficient matrix B
            schema (specified by the normal_eqn_struct):
                 |-- header_block: string
                 |-- sample_block: string
                 |-- header: string
                 |-- sort_key: integer
                 |-- alphas: array
                 |    |-- element: string
                 |-- labels: array
                 |    |-- element: string
                 |-- coefficients: array
                 |    |-- element: double
    """

    header_block, sample_block, label = parse_key(key, key_pattern)
    sort_in_place(pdf, ['sort_key', 'header'])
    alpha_names, alpha_values = zip(*sorted(alphas.items()))
    if covdf.empty:
        beta_stack = evaluate_coefficients(pdf, alpha_values, 0)
    else:
        beta_stack = evaluate_coefficients(pdf, alpha_values, len(covdf.columns))
    row_indexer = cross_alphas_and_labels(alpha_names, labeldf, label)
    alpha_row, label_row = zip(*row_indexer)
    output_length = len(pdf)
    data = {
        'header_block': header_block,
        'sample_block': sample_block,
        'header': pdf['header'],
        'sort_key': pdf['sort_key'],
        'alphas': [list(alpha_row)] * output_length,
        'labels': [list(label_row)] * output_length,
        'coefficients': list(beta_stack)
    }

    return pd.DataFrame(data)


@typechecked
def apply_model(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                sample_index: Dict[str, List[str]], alphas: Dict[str, Float],
                covdf: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes a block X and a coefficient matrix B and performs the multiplication X*B.  The matrix resulting
    from this multiplication represents a block in a new, dimensionally-reduced block matrix.

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame containing the lists of rows used to assemble block X and coefficients B
            identified by :key:
            schema:
                 |-- header_block: string
                 |-- sample_block: string
                 |-- header: string
                 |-- size: integer
                 |-- indices: array
                 |    |-- element: integer
                 |-- values: array
                 |    |-- element: double
                 |-- sort_key: integer
                 |-- mu: double
                 |-- sig: double
                 |-- alphas: array
                 |    |-- element: string
                 |-- labels: array
                 |    |-- element: string
                 |-- coefficients: array
                 |    |-- element: double
        labeldf : Pandas DataFrame containing label values that were used in fitting coefficient matrix B.
        sample_index : sample_index: dict containing a mapping of sample_block ID to a list of corresponding sample IDs
        alphas : dict of {alphaName : alphaValue} for the alpha values that were used when fitting coefficient matrix B
        covdf: Pandas DataFrame containing covariates that should be included with every block X above (can be empty).

    Returns:
        transformed Pandas DataFrame containing reduced matrix block produced by the multiplication X*B
            schema (specified by reduced_matrix_struct):
                 |-- header: string
                 |-- size: integer
                 |-- values: array
                 |    |-- element: double
                 |-- header_block: string
                 |-- sample_block: string
                 |-- sort_key: integer
                 |-- mu: double
                 |-- sig: double
                 |-- alpha: string
                 |-- label: string
    """

    header_block, sample_block, label = parse_key(key, key_pattern)
    sort_in_place(pdf, ['sort_key'])

    if covdf.empty:
        n_rows = pdf['size'][0]
        n_cols = len(pdf)
        X = assemble_block(n_rows, n_cols, pdf, np.array([]))
    else:
        sample_list = sample_index[sample_block]
        n_rows = int(pdf[~pdf['values'].isnull()]['size'].array[0])
        n_cols = len(pdf[~pdf['values'].isnull()])
        cov_matrix = slice_label_rows(covdf, 'all', sample_list)
        X = assemble_block(n_rows, n_cols, pdf[~pdf['values'].isnull()], cov_matrix)

    B = np.row_stack(pdf['coefficients'].array)
    XB = X @ B
    mu, sig = XB.mean(axis=0), XB.std(axis=0)
    alpha_names = sorted(alphas.keys())
    row_indexer = cross_alphas_and_labels(alpha_names, labeldf, label)
    alpha_col, label_col = zip(*row_indexer)
    new_header_block, sort_key_col, header_col = new_headers(header_block, alpha_names, row_indexer)

    data = {
        'header': header_col,
        'size': X.shape[0],
        'values': list(XB.T),
        'header_block': new_header_block,
        'sample_block': sample_block,
        'sort_key': sort_key_col,
        'mu': mu,
        'sig': sig,
        'alpha': alpha_col,
        'label': label_col
    }

    return pd.DataFrame(data)


@typechecked
def score_models(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                 sample_index: Dict[str, List[str]], alphas: Dict[str, Float],
                 covdf: pd.DataFrame) -> pd.DataFrame:
    """
    Similar to apply_model, this function performs the multiplication X*B for a block X and corresponding coefficient
    matrix B, however it also evaluates the coefficient of determination (r2) for each of columns in B against the
    corresponding label.

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame containing the lists of rows used to assemble block X and coefficients B
            identified by :key:
            schema:
                 |-- header_block: string
                 |-- sample_block: string
                 |-- header: string
                 |-- size: integer
                 |-- indices: array
                 |    |-- element: integer
                 |-- values: array
                 |    |-- element: double
                 |-- sort_key: integer
                 |-- mu: double
                 |-- sig: double
                 |-- alphas: array
                 |    |-- element: string
                 |-- labels: array
                 |    |-- element: string
                 |-- coefficients: array
                 |    |-- element: double
        labeldf : Pandas DataFrame containing label values that were used in fitting coefficient matrix B.
        sample_index : sample_index: dict containing a mapping of sample_block ID to a list of corresponding sample IDs
        alphas : dict of {alphaName : alphaValue} for the alpha values that were used when fitting coefficient matrix B
        covdf: Pandas DataFrame containing covariates that should be included with every block X above (can be empty).

    Returns:
        Pandas DataFrame containing the r2 scores for each combination of alpha and label
            schema:
                 |-- sample_block: string
                 |-- label: string
                 |-- alpha: string
                 |-- r2: double
    """
    header_block, sample_block, label = parse_key(key, key_pattern)
    sort_in_place(pdf, ['sort_key'])
    sample_list = sample_index[sample_block]

    if covdf.empty:
        n_rows = pdf['size'][0]
        n_cols = len(pdf)
        X = assemble_block(n_rows, n_cols, pdf, np.array([]))
    else:
        # If there is a covdf, we will have null 'values' entries in pdf arising from the right join of blockdf
        # to modeldf, so we will filter those rows out before assembling the block.
        sample_list = sample_index[sample_block]
        n_rows = int(pdf[~pdf['values'].isnull()]['size'].array[0])
        n_cols = len(pdf[~pdf['values'].isnull()])
        cov_matrix = slice_label_rows(covdf, 'all', sample_list)
        X = assemble_block(n_rows, n_cols, pdf[~pdf['values'].isnull()], cov_matrix)

    B = np.row_stack(pdf['coefficients'].array)
    XB = X @ B
    Y = slice_label_rows(labeldf, label, sample_list)
    scores = r_squared(XB, Y)
    alpha_names = sorted(alphas.keys())

    data = {'sample_block': sample_block, 'label': label, 'alpha': alpha_names, 'r2': scores}

    return pd.DataFrame(data)
