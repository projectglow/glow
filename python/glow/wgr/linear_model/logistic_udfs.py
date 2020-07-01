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
                 sample_blocks: Dict[str, List[str]], covdf: pd.DataFrame,
                 p0_dict: Dict[str, Float], alphas: Dict[str, Float]) -> pd.DataFrame:
    header_block, sample_block, label, alpha_name = parse_key(key, key_pattern)
    sort_in_place(pdf, ['sort_key', 'header'])
    n_rows = pdf['size'][0]
    n_cols = len(pdf)
    sample_list = sample_blocks[sample_block]
    p0 = p0_dict[label]
    alpha_value = alphas[alpha_name]
    cov_matrix = slice_label_rows(covdf, 'all', sample_list)
    n_cov = len(covdf.columns)
    header_col = np.concatenate([covdf.columns, pdf['header']])
    #Add new sort_keys for covariates, starting from -n_cov up to 0 to ensure they come ahead of the headers.
    sort_key_col = np.concatenate((np.arange(-n_cov, 0), pdf['sort_key']))
    X = assemble_block(n_rows, n_cols, pdf, cov_matrix)

    Y = slice_label_rows(labeldf, label, sample_list)

    beta, XtGX, XtY = get_irls_pieces(X, Y.ravel(), alpha_value, p0)

    data = {
        "header_block" : header_block,
        "sample_block" : sample_block,
        "label" : label,
        "alpha_name" : alpha_name,
        "header" : header_col,
        "sort_key" : sort_key_col,
        "beta" : list(beta),
        "xtgx" : list(XtGX),
        "xty": list(XtY)
    }

    return pd.DataFrame(data)


@typechecked
def reduce_irls_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame) -> pd.DataFrame:
    sum_xtgx = pdf['xtgx'].sum()
    sum_xty = pdf['xty'].sum()
    mean_beta = pdf['beta'].mean()

    # Use numpy broadcast to subtract each row from the sum
    pdf['xtgx'] = list(sum_xtgx - np.vstack(pdf['xtgx'].array))
    pdf['xty'] = list(sum_xty - pdf['xty'])
    pdf['beta'] = list((pdf.shape[0]*mean_beta - pdf['beta'])/(pdf.shape[0]-1))

    return pdf


@typechecked
def solve_irls_eqn(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                   alphas: Dict[str, Float], covdf: pd.DataFrame) -> pd.DataFrame:
    header_block, sample_block, label = parse_key(key, key_pattern)
    sort_in_place(pdf, ['alpha_name','sort_key', 'header'])
    alpha_names, alpha_values = zip(*sorted(alphas.items()))
    n_cov = len(covdf.columns)
    rows_per_alpha = int(len(pdf)/len(alpha_values))
    beta_stack = irls_one_step(pdf, alpha_values, n_cov, rows_per_alpha)
    row_indexer = cross_alphas_and_labels(alpha_names, labeldf, label)
    alpha_row, label_row = zip(*row_indexer)
    data = {
        'header_block': header_block,
        'sample_block': sample_block,
        'header': pdf['header'][:rows_per_alpha],
        'sort_key': pdf['sort_key'][:rows_per_alpha],
        'alphas': [list(alpha_row)] * rows_per_alpha,
        'labels': [list(label_row)] * rows_per_alpha,
        'coefficients': list(beta_stack)
    }
    return pd.DataFrame(data)


@typechecked
def apply_logistic_model(key: Tuple, key_pattern: List[str], pdf: pd.DataFrame, labeldf: pd.DataFrame,
                         sample_blocks: Dict[str, List[str]], alphas: Dict[str, Float],
                         covdf: pd.DataFrame) -> pd.DataFrame:
    header_block, sample_block, label = parse_key(key, key_pattern)
    sort_in_place(pdf, ['sort_key'])
    # If there is a covdf, we will have null 'values' entries in pdf arising from the right join of blockdf
    # to modeldf, so we will filter those rows out before assembling the block.
    sample_list = sample_blocks[sample_block]
    n_rows = int(pdf[~pdf['values'].isnull()]['size'].array[0])
    n_cols = len(pdf[~pdf['values'].isnull()])
    cov_matrix = slice_label_rows(covdf, 'all', sample_list)
    X = assemble_block(n_rows, n_cols, pdf[~pdf['values'].isnull()], cov_matrix)
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