from .functions import *
from pyspark.sql.types import ArrayType, IntegerType, FloatType, StructType, StructField, StringType, DoubleType
'''
Each function in this module performs a Pandas DataFrame => Pandas DataFrame transformation, and each is intended to be
used as a Pandas GROUPED_MAP UDF.
'''
normal_eqn_struct = StructType([
    StructField('header_block', StringType()),
    StructField('sample_block', IntegerType()),
    StructField('label', StringType()),
    StructField('header', StringType()),
    StructField('position', IntegerType()),
    StructField('xtx', ArrayType(DoubleType())),
    StructField('xty', ArrayType(DoubleType()))
])

model_struct = StructType([
    StructField('header_block', StringType()),
    StructField('sample_block', IntegerType()),
    StructField('header', StringType()),
    StructField('position', IntegerType()),
    StructField('alphas', ArrayType(StringType())),
    StructField('labels', ArrayType(StringType())),
    StructField('coefficients', ArrayType(DoubleType()))
])

reduced_matrix_struct = StructType([
    StructField('header', StringType()),
    StructField('size', IntegerType()),
    StructField('values', ArrayType(DoubleType())),
    StructField('header_block', StringType()),
    StructField('sample_block', IntegerType()),
    StructField('position', IntegerType()),
    StructField('mu', DoubleType()),
    StructField('sig', DoubleType()),
    StructField('alpha', StringType()),
    StructField('label', StringType())
])

cv_struct = StructType([
    StructField('sample_block', IntegerType()),
    StructField('label', StringType()),
    StructField('alpha', StringType()),
    StructField('r2', DoubleType())
])


def map_normal_eqn(key, key_pattern, pdf, labeldf, sample_index):
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
             |-- sample_block: integer
             |-- position: integer
             |-- mu: double
             |-- sig: double
             |-- alpha: double (Required only if the header is tied to a specific value of alpha)
             |-- label: double (Required only if the header is tied to a specific label)
        labeldf : Pandas DataFrame containing label values (i. e., the Y in the normal equation above).

    Returns:
        transformed Pandas DataFrame containing XtX and XtY corresponding to a particular block X.
            schema (specified by the normal_eqn_struct):
             |-- header_block: string
             |-- sample_block: integer
             |-- label: string
             |-- header: string
             |-- position: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
    """
    header_block, sample_block, label = parse_key(key, key_pattern)
    n_rows = pdf['size'][0]
    n_cols = len(pdf)
    position = pdf['position']
    header = pdf['header']
    col_order = [(t[0]) for t in sorted(list(enumerate(zip(position, header))), key = lambda t: (t[1][0], t[1][1]))]
    sample_list = sample_index[sample_block]
    X = assemble_block(n_rows, n_cols, col_order, pdf)
    Y = slice_label_rows(labeldf, label, sample_list)
    XtX = X.T@X
    XtY = X.T@Y

    data = {
        'header_block' : [header_block]*n_cols,
        'sample_block' : [sample_block]*n_cols,
        'label' : [label]*n_cols,
        'header' : header[col_order].values,
        'position' : position[col_order].values,
        'xtx' : XtX.tolist(),
        'xty' : XtY.tolist()
    }

    return pd.DataFrame(data)


def reduce_normal_eqn(key, key_pattern, pdf):
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
             |-- sample_block: integer
             |-- label: string
             |-- header: string
             |-- position: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double

    Returns:
        transformed Pandas DataFrame containing the aggregated leave-fold-out rows from XtX and XtY
            schema (specified by the normal_eqn_struct):
             |-- header_block: string
             |-- sample_block: integer
             |-- label: string
             |-- header: string
             |-- position: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
    """

    header_block, header, label = parse_key(key, key_pattern)
    position = pdf['position'][0]
    n_sample_blocks = len(pdf)
    sample_blocks = enumerate(pdf['sample_block'])
    slices = [(g, np.append(np.arange(i), np.arange(i+1, n_sample_blocks))) for i, g in sample_blocks]
    xtx_stack = np.stack(pdf['xtx'].values)
    xty_stack = np.stack(pdf['xty'].values)

    rows = [[header_block, g, label, header, position, xtx_stack[s, :].sum(axis = 0), xty_stack[s, :].sum(axis = 0)] for g, s in slices]

    return pd.DataFrame(rows, columns = ['header_block', 'sample_block', 'label', 'header', 'position', 'xtx', 'xty'])


def solve_normal_eqn(key, key_pattern, pdf, labeldf, alphas):
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
             |-- sample_block: integer
             |-- label: string
             |-- header: string
             |-- position: integer
             |-- xtx: array
             |    |-- element: double
             |-- xty: array
             |    |-- element: double
        labeldf : Pandas DataFrame containing label values (i. e., the Y in the normal equation above).
        alphas : dict of {alphaName : alphaValue} for the alpha values to be used

    Returns:
        transformed Pandas DataFrame containing the coefficient matrix B
            schema (specified by the normal_eqn_struct):
                 |-- header_block: string
                 |-- sample_block: integer
                 |-- header: string
                 |-- position: integer
                 |-- alphas: array
                 |    |-- element: string
                 |-- labels: array
                 |    |-- element: string
                 |-- coefficients: array
                 |    |-- element: double
    """

    header_block, sample_block, label = parse_key(key, key_pattern)
    position = pdf['position']
    header = pdf['header']
    row_order = [(t[0]) for t in sorted(list(enumerate(zip(position, header))), key = lambda t: (t[1][0], t[1][1]))]
    alpha_names, alpha_values = zip(*[(k,v) for k, v in sorted(alphas.items(), key = lambda t: t[0])])
    beta_stack = evaluate_coefficients(pdf, row_order, alpha_values)
    row_indexer = create_row_indexer(alpha_names, labeldf, label)
    col_order, alpha_row, label_row = zip(*[(i,a,l) for i,(a,l) in row_indexer])
    output_length = len(pdf)
    data = {
        'header_block' : [header_block]*output_length,
        'sample_block' : [sample_block]*output_length,
        'header' : header[row_order],
        'position' : position[row_order],
        'alphas' : [list(alpha_row)]*output_length,
        'labels' : [list(label_row)]*output_length,
        'coefficients' : [r[list(col_order)].tolist() for r in beta_stack]
    }

    return pd.DataFrame(data)


def apply_model(key, key_pattern, pdf, labeldf, alphas):
    """
    This function takes a block X and a coefficient matrix B and performs the multiplication X*B.  The matrix resulting
    from this multiplication represents a block in a new, dimensionally-reduced block matrix.

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame containing the lists of rows used to assemble block X and coeffients B
            identified by :key:
            schema:
                 |-- header_block: string
                 |-- sample_block: integer
                 |-- header: string
                 |-- size: integer
                 |-- indices: array
                 |    |-- element: integer
                 |-- values: array
                 |    |-- element: double
                 |-- position: integer
                 |-- mu: double
                 |-- sig: double
                 |-- alphas: array
                 |    |-- element: string
                 |-- labels: array
                 |    |-- element: string
                 |-- coefficients: array
                 |    |-- element: double
        labeldf : Pandas DataFrame containing label values that were used in fitting coefficient matrix B.
        alphas : dict of {alphaName : alphaValue} for the alpha values that were used when fitting coefficient matrix B

    Returns:
        transformed Pandas DataFrame containing reduced matrix block produced by the multiplication X*B
            schema (specified by reduced_matrix_struct):
                 |-- header: string
                 |-- size: integer
                 |-- values: array
                 |    |-- element: double
                 |-- header_block: string
                 |-- sample_block: integer
                 |-- position: integer
                 |-- mu: double
                 |-- sig: double
                 |-- alpha: string
                 |-- label: string
    """

    header_block, sample_block, label = parse_key(key, key_pattern)
    n_rows = pdf['size'][0]
    n_cols = len(pdf)
    position = pdf['position']
    col_order = [(t[0]) for t in sorted(list(enumerate(position)), key = lambda t: t[1])]
    X = assemble_block(n_rows, n_cols, col_order, pdf)
    B = np.row_stack(pdf['coefficients'][col_order].values)
    XB = np.asarray(X@B)
    mu, sig = XB.mean(axis = 0).tolist(), XB.std(axis = 0).tolist()
    alpha_names = [k for k, v in sorted(alphas.items(), key = lambda t: t[0])]
    row_indexer = create_row_indexer(alpha_names, labeldf, label)
    alpha_col, label_col = zip(*[(a, l) for i, (a, l) in row_indexer])
    new_header_block, position_col, header_col = new_headers(header_block, alpha_names, row_indexer)
    n_output_rows = len(row_indexer)

    data = {
        'header' : header_col,
        'size' : [X.shape[0]]*n_output_rows,
        'values' : XB.T.tolist(),
        'header_block' : [new_header_block]*n_output_rows,
        'sample_block' : [sample_block]*n_output_rows,
        'position' : position_col,
        'mu' : mu,
        'sig' : sig,
        'alpha' : alpha_col,
        'label' : label_col
    }

    return pd.DataFrame(data)


def score_models(key, key_pattern, pdf, labeldf, sample_index, alphas):
    """
    Similar to apply_model, this function performs the multiplication X*B for a block X and corresponding coefficient
    matrix B, however it also evaluates the coefficient of determination (r2) for each of columns in B against the
    corresponding label.

    Args:
        key : unique key identifying the group of rows emitted by a groupBy statement
        key_pattern : pattern of columns used in the groupBy statement that emitted this group of rows
        pdf : starting Pandas DataFrame containing the lists of rows used to assemble block X and coeffients B
            identified by :key:
            schema:
                 |-- header_block: string
                 |-- sample_block: integer
                 |-- header: string
                 |-- size: integer
                 |-- indices: array
                 |    |-- element: integer
                 |-- values: array
                 |    |-- element: double
                 |-- position: integer
                 |-- mu: double
                 |-- sig: double
                 |-- alphas: array
                 |    |-- element: string
                 |-- labels: array
                 |    |-- element: string
                 |-- coefficients: array
                 |    |-- element: double
        labeldf : Pandas DataFrame containing label values that were used in fitting coefficient matrix B.
        alphas : dict of {alphaName : alphaValue} for the alpha values that were used when fitting coefficient matrix B

    Returns:
        Pandas DataFrame containing the r2 scores for each combination of alpha and label
            schema:
                 |-- sample_block: integer
                 |-- label: string
                 |-- alpha: string
                 |-- r2: double
    """
    header_block, sample_block, label = parse_key(key, key_pattern)
    n_rows = pdf['size'][0]
    n_cols = len(pdf)
    position = pdf['position']
    col_order = [(t[0]) for t in sorted(list(enumerate(position)), key = lambda t: t[1])]
    sample_list = sample_index[sample_block]
    X = assemble_block(n_rows, n_cols, col_order, pdf)
    B = np.row_stack(pdf['coefficients'][col_order].values)
    XB = np.asarray(X@B)
    Y = slice_label_rows(labeldf, label, sample_list)
    scores = r_squared(XB, Y)
    alpha_names = [k for k, v in sorted(alphas.items(), key = lambda t: t[0])]
    n_output_rows = len(alpha_names)

    data = {
        'sample_block' : [sample_block]*n_output_rows,
        'label' : [label]*n_output_rows,
        'alpha' : alpha_names,
        'r2' : scores
    }

    return pd.DataFrame(data)
