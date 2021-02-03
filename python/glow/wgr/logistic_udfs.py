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

from .ridge_udfs import *

irls_eqn_struct = StructType([
    StructField('header_block', StringType()),
    StructField('sample_block', StringType()),
    StructField('label', StringType()),
    StructField('alpha_name', StringType()),
    StructField('header', StringType()),
    StructField('sort_key', IntegerType()),
    StructField('beta', DoubleType()),
    StructField('xtgx', ArrayType(DoubleType())),
    StructField('xty', DoubleType())
])

logistic_reduced_matrix_struct = StructType([
    StructField('header', StringType()),
    StructField('size', IntegerType()),
    StructField('values', ArrayType(DoubleType())),
    StructField('header_block', StringType()),
    StructField('sample_block', StringType()),
    StructField('sort_key', IntegerType()),
    StructField('alpha', StringType()),
    StructField('label', StringType())
])


@typechecked
def map_irls_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                 sample_blocks: Dict[str,
                                     List[str]], covdf: pd.DataFrame, beta_cov_dict: Dict[str,
                                                                                          NDArray],
                 maskdf: pd.DataFrame, alphas: Dict[str, Float]) -> pd.DataFrame:
    """
    This function constructs matrices X and Y, and computes transpose(X)*diag(p(1-p)*X, beta, and transpose(X)*Y, by
    fitting a logistic model logit(p(Y|X)) ~ X*beta.

    Each block X is uniquely identified by a header_block ID, which maps to a set of contiguous columns in the overall
    block matrix, and a sample_block ID, which maps to a set of rows in the overall block matrix (and likewise a set
    of rows in the label matrix Y).  Additionally, each block is associated with a particular label and alpha,
    so the key for the group is (header_block, sample_block, label, alpha).

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
             |-- label: string
             |-- alpha_name: string
        labeldf : Pandas DataFrame containing label values (i. e., the Y in the normal equation above).
        sample_index : sample_index: dict containing a mapping of sample_block ID to a list of corresponding sample IDs
        covdf : Pandas DataFrame containing covariates that should be included with every block X above (can be empty).
        beta_cov_dict : dict of [label: str, beta: NDArray[Float]] that maps each label to the covariate parameter
            values estimated from the entire population.
        maskdf : Pandas DataFrame mirroring labeldf containing Boolean values flagging samples with missing labels as
            True and others as False.
        alphas : dict of [alpha_name: str, alpha_value: float]

    Returns:
        transformed Pandas DataFrame containing beta, XtgX, and XtY corresponding to a particular block X.
            schema (specified by the irls_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- alpha_name : string
             |-- header: string
             |-- sort_key: integer
             |-- beta : double
             |-- xtgx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
    """
    header_block, sample_block, label, alpha_name = parse_header_block_sample_block_label_alpha_name(
        key, key_pattern)
    sort_in_place(pdf, ['sort_key', 'header'])
    n_rows = pdf['size'][0]
    n_cols = len(pdf)
    sample_list = sample_blocks[sample_block]
    beta_cov = beta_cov_dict[label]

    if maskdf.empty:
        row_mask = np.array([])
    else:
        row_mask = slice_label_rows(maskdf, label, sample_list, np.array([])).ravel()

    alpha_value = alphas[alpha_name]
    cov_matrix = np.array([]) if covdf.empty else slice_label_rows(covdf, 'all', sample_list,
                                                                   np.array([]))
    n_cov = len(covdf.columns)
    header_col = np.concatenate([covdf.columns, pdf['header']])
    #Add new sort_keys for covariates, starting from -n_cov up to 0 to ensure they come ahead of the headers.
    sort_key_col = np.concatenate((np.arange(-n_cov, 0), pdf['sort_key']))
    X = assemble_block(n_rows, n_cols, pdf, cov_matrix, row_mask)

    Y = slice_label_rows(labeldf, label, sample_list, row_mask)

    beta, XtGX, XtY = get_irls_pieces(X, Y.ravel(), alpha_value, beta_cov)

    data = {
        "header_block": header_block,
        "sample_block": sample_block,
        "label": label,
        "alpha_name": alpha_name,
        "header": header_col,
        "sort_key": sort_key_col,
        "beta": list(beta),
        "xtgx": list(XtGX),
        "xty": list(XtY)
    }

    return pd.DataFrame(data)


@typechecked
def reduce_irls_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame) -> pd.DataFrame:
    """
    This function constructs lists of rows from the beta, XtGX, and XtY matrices corresponding to a particular header
    in X and alpha value evaluated in different sample_blocks, and then reduces those lists by element-wise mean in the
    case of beta and element-wise summation in the case of xtgx and xty. This reduction is repeated once for
    each sample_block, where the contribution of that sample_block is omitted.  There is therefore a one-to-one
    mapping of the starting lists and the reduced lists, e.g.:

        Input:
            List(xtgx_sample_block0, xtgx_sample_block1, ..., xtgx_sample_blockN)
        Output:
            List(xtgx_sum_excluding_sample_block0, xtgx_sum_excluding_sample_block1, ..., xtgx_sum_excluding_sample_blockN)

    Args:
        key : unique key identifying the rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement
        pdf : starting Pandas DataFrame containing the lists of rows from XtX and XtY for block X identified by :key:
            schema (specified by the irls_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- alpha_name : string
             |-- header: string
             |-- sort_key: integer
             |-- beta : double
             |-- xtgx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double

    Returns:
        transformed Pandas DataFrame containing the aggregated leave-fold-out rows from XtX and XtY
            schema (specified by the irls_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- alpha_name : string
             |-- header: string
             |-- sort_key: integer
             |-- beta : double
             |-- xtgx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
    """
    sum_xtgx = pdf['xtgx'].sum()
    sum_xty = pdf['xty'].sum()
    mean_beta = pdf['beta'].mean()

    # Use numpy broadcast to subtract each row from the sum
    pdf['xtgx'] = list(sum_xtgx - np.vstack(pdf['xtgx'].array))
    pdf['xty'] = list(sum_xty - pdf['xty'])
    pdf['beta'] = list((pdf.shape[0] * mean_beta - pdf['beta']) / (pdf.shape[0] - 1))

    return pdf


@typechecked
def solve_irls_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                   alphas: Dict[str, Float], covdf: pd.DataFrame) -> pd.DataFrame:
    """
    This function assembles the matrices XtGX, XtY and initial parameter guess B0 for a particular sample_block
    (where the contribution of that sample_block has been omitted) and solves the equation
    B = B0 - [(XtGX + I*alpha)]-1 * XtY for a single alpha value, and returns the coefficient vector B, where B has 1
    element per header in the block X.

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame containing the lists of rows from beta, XtGX, and XtY for block X identified
        by :key:
            schema (specified by the irls_eqn_struct):
             |-- header_block: string
             |-- sample_block: string
             |-- label: string
             |-- alpha_name : string
             |-- header: string
             |-- sort_key: integer
             |-- beta : double
             |-- xtgx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
        labeldf : Pandas DataFrame containing label values (i. e., the Y in the normal equation above).
        alphas : dict of {alphaName : alphaValue} for the alpha values to be used
        covdf : Pandas DataFrame containing covariates that should be included with every block X above (can be empty).

    Returns:
        transformed Pandas DataFrame containing the coefficient matrix B
            schema (specified by the model_struct):
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
    header_block, sample_block, label, alpha_name = parse_header_block_sample_block_label_alpha_name(
        key, key_pattern)
    sort_in_place(pdf, ['sort_key', 'header'])
    alpha_value = alphas[alpha_name]
    n_cov = len(covdf.columns)
    beta = irls_one_step(pdf, alpha_value, n_cov)
    row_indexer = cross_alphas_and_labels([alpha_name], labeldf, label)
    alpha_row, label_row = zip(*row_indexer)
    output_length = len(pdf)
    data = {
        'header_block': header_block,
        'sample_block': sample_block,
        'header': pdf['header'],
        'sort_key': pdf['sort_key'],
        'alphas': [list(alpha_row)] * output_length,
        'labels': [list(label_row)] * output_length,
        'coefficients': list(beta.reshape(-1, 1))
    }
    return pd.DataFrame(data)


@typechecked
def apply_logistic_model(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame,
                         labeldf: pd.DataFrame, sample_blocks: Dict[str, List[str]],
                         alphas: Dict[str, Float], covdf: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes a block X and a coefficient matrix B, performs the multiplication X*B, and returns sigmoid(X*B),
    representing the output of the logistic model p(y|X) = sigmoid(XB).

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
            schema (specified by logistic_reduced_matrix_struct):
                 |-- header: string
                 |-- size: integer
                 |-- values: array
                 |    |-- element: double
                 |-- header_block: string
                 |-- sample_block: string
                 |-- sort_key: integer
                 |-- alpha: string
                 |-- label: string
    """
    header_block, sample_block, label = parse_header_block_sample_block_label(key, key_pattern)
    sort_in_place(pdf, ['sort_key'])
    # If there is a covdf, we will have null 'values' entries in pdf arising from the right join of blockdf
    # to modeldf, so we will filter those rows out before assembling the block.
    sample_list = sample_blocks[sample_block]
    n_rows = int(pdf[~pdf['values'].isnull()]['size'].array[0])
    n_cols = len(pdf[~pdf['values'].isnull()])
    cov_matrix = slice_label_rows(covdf, 'all', sample_list, np.array([]))
    X = assemble_block(n_rows, n_cols, pdf[~pdf['values'].isnull()], cov_matrix, np.array([]))
    B = np.row_stack(pdf['coefficients'].array)
    XB = X @ B
    P = sigmoid(XB)
    alpha_names = sorted(alphas.keys())
    row_indexer = cross_alphas_and_labels(alpha_names, labeldf, label)
    alpha_col, label_col = zip(*row_indexer)
    new_header_block, sort_key_col, header_col = new_headers(header_block, alpha_names, row_indexer)
    data = {
        'header': header_col,
        'size': X.shape[0],
        'values': list(P.T),
        'header_block': new_header_block,
        'sample_block': sample_block,
        'sort_key': sort_key_col,
        'alpha': alpha_col,
        'label': label_col
    }

    return pd.DataFrame(data)
