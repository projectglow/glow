import numpy as np
from scipy.sparse import csr_matrix
import itertools


def parse_key(key, key_pattern):
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
    else:
        return key


def assemble_block(n_rows, n_cols, col_order, pdf):
    """
    Creates a dense n_rows by n_cols matrix from the array of either sparse or dense vectors in the Pandas DataFrame
    corresponding to a group.  This matrix represents a block.

    Args:
         n_rows : The number of rows in the resulting matrix
         n_cols : The number of columns in the resulting matrix
         col_order : Array of integers representing the desired ordering of the columns in the output matrix
         pdf : Pandas DataFrame corresponding to a group

    Returns:
        Dense n_rows by n_columns matrix where the columns have been 0-centered and standard scaled.
    """
    mu = pdf['mu'].values
    sig = pdf['sig'].values
    if 'indices' not in pdf.columns:
        X_raw = np.row_stack(pdf['values']).T
    else:
        X_csr = csr_matrix(
            (
                np.concatenate(pdf['values']),
                (np.concatenate(pdf['indices']), np.concatenate([np.repeat(i, len(v)) for i, v in enumerate(pdf.indices)]))
            ),
            shape = (n_rows, n_cols)
        )
        X_raw = X_csr.todense()

    return ((X_raw - mu)/sig)[:, col_order]


def slice_label_rows(labeldf, label, sample_list):
    """
    Selects rows from the Pandas DataFrame of labels corresponding to the samples in a particular sample_block.

    Args:
        pdf : Pandas DataFrame for the group
        labeldf : Pandas DataFrame containing the labels
        label : Header for the particular label to slice.  Can be 'all' if all labels are desired.
        sample_list : List of sample ids corresponding to the sampleBlock to be sliced out.

    Returns:
        Matrix of [number of samples in sample_block] x [number of labels to slice]
    """
    if label == 'all':
        return labeldf.loc[sample_list, :].values
    else:
        return labeldf[label].loc[sample_list].values.reshape(-1, 1)


def evaluate_coefficients(pdf, row_order, alpha_values):
    """
    Solves the system (XTX + Ia)^-1 * XtY for each of the a values in alphas.  Returns the resulting coefficients.

    Args:
         pdf : Pandas DataFrame for the group
         row_order : Array of integers representing the intended row ordering of the matrices XtX and XtY
         alpha_values : Array of alpha values (regularization strengths)

    Returns:
        Matrix of coefficients of size [number of columns in X] x [number of labels * number of alpha values]
    """
    XtX = np.stack(pdf['xtx'].values)[row_order]
    XtY = np.stack(pdf['xty'].values)[row_order]
    return np.column_stack([(np.linalg.inv(XtX + np.identity(XtX.shape[1])*a)@XtY) for a in alpha_values])


def create_row_indexer(alpha_names, labeldf, label):
    """
    Creates an array of tuples used to keep the ordering of the coefficients in the output of evaluate_coefficients in
    a consistent order regardless of the order in which the alpha values and labels were provided.

    Args:
        alpha_names : List of string identifiers assigned to the values of alpha
        labeldf : Pandas DataFrame of labels
        label : Label used for this set of coefficients.  Can be 'all' if all labels were used.
    Returns:
        List of tuples of the form (i,(a, l)) where i is the column index in the matrix of coefficients, a is an alpha
        name, and l is a label name, sorted by label name, then alpha.
    """
    if label == 'all':
        label_names = labeldf.columns
    else:
        label_names = [label]

    return sorted(enumerate(itertools.product(alpha_names, label_names)), key = lambda t: (t[1][1], t[1][0]))


def new_headers(header_block, alpha_names, row_indexer):
    """
    Creates new headers for the output of a matrix reduction step.  Generally produces names like
    "block_[header_block_number]_alpha_[alpha_name]_label_[label_name]"

    Args:
        header_block : Identifier for a header_block (e.g., 'chr1_block_0')
        alpha_names : List of string identifiers for alpha parameters
        row_indexer : A list of tuples provided by the create_row_indexer function

    Returns:
        new_header_block : A new header_block name, typically the chromosome (e.g. chr1), but might be 'all' if there are no more levels to
        reduce over.
        sort_keys : Array of sortable integers to specify the ordering of the new matrix headers.
        headers : List of new matrix headers.
    """
    tokens = header_block.split('_')

    if len(tokens) == 2:
        outer_index, inner_index = 'all', tokens[1]
        new_header_block = f'{outer_index}'
    elif len(tokens) == 1:
        outer_index, inner_index = 'all', 0
        new_header_block = f'{outer_index}'
    else:
        outer_index, inner_index = tokens[1:4:2]
        new_header_block = f'chr_{outer_index}'

    sort_keys, headers = [], []
    for i, (a, l) in row_indexer:
        sort_key = int(inner_index)*len(alpha_names) + int(a.split('_')[1])
        header = f'{new_header_block}_block_{inner_index}_{a}_label_{l}'
        sort_keys.append(sort_key)
        headers.append(header)

    return new_header_block, sort_keys, headers


def r_squared(XB, Y):
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
    res = np.power(Y - XB, 2).sum(axis = 0)
    return 1 - (res/tot)
