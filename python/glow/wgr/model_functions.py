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
import re
import warnings
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy.optimize
from nptyping import Float, Int, NDArray
from pyspark.sql import DataFrame, Row
from pyspark.sql.window import Window
from typeguard import typechecked


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
def parse_header_block_sample_block_label(key: Tuple,
                                          key_pattern: List[str]) -> Tuple[str, str, str]:
    """
    Extracts header_block, sample_block, and label from the key corresponding to a group from the groupBy(key_pattern)
    clause.  Key pattern can be one of:
        (header_block, sample_block),
        (header_block, sample_block, label),
        (sample_block, label)
    depending on the context.  In each case, a tuple with 3 members is returned, with the missing member filled in by
    'all' where necessary

    Args:
        key : key for the group
        key_pattern : one of the aforementioned key patterns

    Returns:
        tuple of (header_block, sample_block, label), where header_block or label may be filled with 'all'
        depending on context.
    """
    if 'label' in key_pattern:
        label = key[-1]
    else:
        label = 'all'
    if 'header_block' in key_pattern:
        header_block = key[0]
        sample_block = key[1]
    else:
        header_block = 'all'
        sample_block = key[0]

    return header_block, sample_block, label


@typechecked
def parse_header_block_sample_block_label_alpha_name(
        key: Tuple, key_pattern: List[str]) -> Tuple[str, str, str, str]:
    """
    Extracts header_block, sample_block, label, and alpha_name from the key corresponding to a group from the
    groupBy(key_pattern) clause.  Key pattern can be one of:
        (header_block, sample_block, label, alpha_name),
        (sample_block, label, alpha_name)
    depending on the context.  In each case, a tuple with 4 members is returned, with header_block filled with 'all' if
    it is absent from they key

    Args:
        key : key for the group
        key_pattern : one of the aforementioned key patterns

    Returns:
        tuple of (header_block, sample_block, label, alpha_name), where header_block or label may be filled with 'all'
        depending on context.
    """
    if 'header_block' in key_pattern:
        return key
    else:
        header_block = 'all'
        return header_block, key[0], key[1], key[2]


# @typechecked
def assemble_block(n_rows: Int, n_cols: Int, pdf: pd.DataFrame, cov_matrix: NDArray[(Any, Any),
                                                                                    Float],
                   row_mask: NDArray[Any]) -> NDArray[Float]:
    """
    Creates a dense n_rows by n_cols matrix from the array of either sparse or dense vectors in the Pandas DataFrame
    corresponding to a group.  This matrix represents a block.

    Args:
         n_rows : The number of rows in the resulting matrix
         n_cols : The number of columns in the resulting matrix
         pdf : Pandas DataFrame corresponding to a group
         cov_matrix: 2D numpy array representing covariate columns that should be prepended to matrix X from the block.  Can be
            empty if covariates are not being applied.
        row_mask:  1D numpy array of size n_rows containing booleans used to mask rows from the block X before
            return.

    Returns:
        Dense n_rows - n_masked by n_columns matrix where the columns have been 0-centered and standard scaled.
    """
    mu = pdf['mu'].to_numpy()
    sig = pdf['sig'].to_numpy()

    if 0 in sig:
        raise ValueError(f'Standard deviation cannot be 0.')

    if row_mask.size == 0:
        row_mask = np.full(n_rows, True)

    if 'indices' not in pdf.columns:
        X_raw = np.column_stack(pdf['values'].array)
    else:
        X_raw = np.zeros([n_rows, n_cols])
        for column, row in enumerate(pdf[['indices', 'values']].itertuples()):
            X_raw[row.indices, column] = row.values

    X = ((X_raw - mu) / sig)

    if cov_matrix.any():
        return np.column_stack((cov_matrix, X))[row_mask, :]
    else:
        return X[row_mask, :]


@typechecked
def constrained_logistic_fit(X: NDArray[Float], y: NDArray[Float], alpha_arr: NDArray[Float],
                             guess: NDArray[Float], n_cov: Int) -> scipy.optimize.OptimizeResult:
    """
    Fits a logistic regression model using design matrix X and binary label y, with ridge penalization specified
    by the penalty parameters in alpha_arr (with one entry per column in X).  The optimization procedure starts from
    the initial state specified in the guess array.  The first n_cov columns in X are assumed to be fixed effects and
    the corresponding parameters are frozen with their initial values in guess.

    Args:
         X: [n_row, n_col] design matrix
         y: [n_row] binary label vector
         alpha_arr : array of n_col ridge penalty values
         guess : array of n_coll initial parameter guesses
         n_cov : number of columns in X (starting from the left) to consider fixed, and freeze at the initial guess
            during optimization.

    Returns:
        scipy optimize result containing optimized paramters from the fit.
    """
    if guess.size == 0:
        guess = np.zeros(X.shape[1])

    def objective(beta):
        z = X @ beta
        p = 1 / (1 + np.exp(-z))
        eps = 1E-15
        ll = (y * np.log(p + eps) + (1 - y) * np.log(1 - p + eps)).sum()
        return -(ll - np.dot(alpha_arr * beta, beta) / 2) / y.size

    def gradient(beta):
        z = X @ beta
        p = 1 / (1 + np.exp(-z))
        grad = -(np.dot((y - p), X) - beta * alpha_arr) / y.size
        if n_cov > 0:
            grad[:n_cov] = 0
        return grad

    return scipy.optimize.minimize(objective, guess, jac=gradient, method='L-BFGS-B')


@typechecked
def get_irls_pieces(X: NDArray[Float], y: NDArray[Float], alpha_value: Float,
                    beta_cov: NDArray[Float]) -> (NDArray[Float], NDArray[Float], NDArray[Float]):
    """
    Fits a logistic regression model logit(p(y|X)) ~ X*beta with L2 shrinkage C = 1/alpha, using scikit-learn.  Returns
    the vector beta, the matrix transpose(X)*diag(p(1-p))*X, and the vector transpose(X)*(y - p), which are the pieces
    needed to perform one step of the IRLS algorithm.

    Args:
         X : Matrix of n samples m features.  Matrix X is expected to include a constant intercept = 1 term as the first
          column.
         y : Binary response vector of  length n
         alpha_value : Shrinkage parameter to be used in the fit
         beta_cov : array of fixed covariate parameter estimates to use in the fitting.

    Returns:
        beta : m length array representing coefficients found from the fit, including the intercept term as the first
        element.
        XtGX : m by m matrix representing transpose(X)*diag(p(1-p))*X.
        XtY : m length array representing transpose(X)*(y - p)

    """
    n_cov = beta_cov.size
    # If we have no observations in this block (i.e, y.sum() == 0), then we should not try to fit a model and instead
    # just return a parameterless model based on the population frequency of observations p0
    if y.sum() > 0:
        alpha_arr = np.zeros(X.shape[1])
        alpha_arr[n_cov:] = alpha_value
        guess = np.zeros(X.shape[1])
        guess[:n_cov] = beta_cov
        ridge_fit = constrained_logistic_fit(X, y, alpha_arr, guess, n_cov)
        beta = ridge_fit.x
    else:
        beta = np.zeros(X.shape[1])
        if n_cov > 0:
            beta[:n_cov] = beta_cov

    z = X @ beta
    p = sigmoid(z)
    XtGX = (X.T * (p * (1 - p))) @ X
    XtY = X.T @ (p - y)
    return beta, XtGX, XtY


# @typechecked
def slice_label_rows(labeldf: pd.DataFrame, label: str, sample_list: List[str],
                     row_mask: NDArray[Any]) -> NDArray[Any]:
    """
    Selects rows from the Pandas DataFrame of labels corresponding to the samples in a particular sample_block.

    Args:
        labeldf : Pandas DataFrame containing the labels
        label : Header for the particular label to slice.  Can be 'all' if all labels are desired.
        sample_list : List of sample ids corresponding to the sample_block to be sliced out.
        row_mask : 1D numpy array of size n_rows containing booleans used to mask samples from the rows sliced from
            labeldf.

    Returns:
        Matrix of [number of samples in sample_block - number of samples masked] x [number of labels to slice]
    """
    if row_mask.size == 0:
        row_mask = np.full(len(sample_list), True)
    if label == 'all':
        return labeldf.loc[sample_list, :].to_numpy()[row_mask, :]
    else:
        return labeldf[label].loc[sample_list].to_numpy().reshape(-1, 1)[row_mask, :]


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
def irls_one_step(pdf: pd.DataFrame, alpha_value: Float, n_cov: Int) -> NDArray[Float]:
    """
    Performs one step of the IRLS algorithm using the components found in pdf and a value of alpha.
    Returns the resulting coefficient values.  If n_cov > 0, then the first n_cov columns from the design matrix X
    are assumed to be fixed effects and the corresponding parameter estimates are not updated during the step.

    Args:
         pdf : Pandas DataFrame for the group.  Contains one set of IRLS equation components (beta, xtgx, xty)
         corresponding to the value of alpha provided
         alpha_value : regularization parameter alpha
         n_cov : number of columns in X (starting from the left) to consider fixed, and freeze at the initial guess
            prior to taking the step.

    Returns:
        vector of coefficients of size [number of columns in X]

    """
    xtgx = np.stack(pdf['xtgx'])[n_cov:, n_cov:]
    xty = pdf['xty'].to_numpy()[n_cov:]
    beta0 = pdf['beta'].to_numpy()
    alpha_arr = np.ones(xtgx.shape[1]) * alpha_value
    dbeta = np.zeros(beta0.size)
    dbeta[n_cov:] = np.linalg.inv(xtgx + np.diag(alpha_arr)) @ (xty + beta0[n_cov:] * alpha_arr)
    return beta0 - dbeta


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
def sigmoid(z: NDArray[Float]) -> NDArray[Float]:
    """
    Computes the sigmoid function for each element in input z

    Args:
        z: Input values

    Returns:
        Sigmoid response corresponding to each input value
    """
    return 1 / (1 + np.exp(-z))


@typechecked
def log_loss(p: NDArray[Float], y: NDArray[Float]) -> NDArray[Float]:
    """
    Computes the log loss of probability values p and observed binary variable y.

    Args:
        p : probability values, assumed to have shape n obervations x m models
        y : observed binary variable, assumed to have shape n observations x 1

    Returns:
        Array of [n * m]
    """
    eps = 1E-15
    return -(y * np.log(p + eps) + (1 - y) * np.log(1 - p + eps)).sum(axis=0) / y.shape[0]


@typechecked
def create_alpha_dict(alphas: NDArray[(Any, ), Float]) -> Dict[str, Float]:
    """
    Creates a mapping to attach string identifiers to alpha values.

    Args:
        alphas : Alpha values

    Returns:
        Dict of [alpha names, alpha values]
    """
    if not (alphas >= 0).all():
        raise Exception('Alpha values must all be non-negative.')
    return {f'alpha_{i}': a for i, a in enumerate(alphas)}


@typechecked
def generate_alphas(blockdf: DataFrame) -> Dict[str, Float]:
    """
    Generates alpha values using a range of heritability values and the number of distinct headers (without labels).

    Args:
        blockdf : Spark DataFrame representing a block matrix
        labeldf: Pandas DataFrame containing target labels

    Returns:
        Dict of [alpha names, alpha values]
    """
    num_label_free_headers = blockdf.select(f.regexp_extract('header', r"^(.+?)($|_label_.*)",
                                                             1)).distinct().count()
    heritability_vals = [0.99, 0.75, 0.50, 0.25, 0.01]
    alphas = np.array([num_label_free_headers / h for h in heritability_vals])
    print(f"Generated alphas: {alphas}")
    return create_alpha_dict(alphas)


@typechecked
def _assert_all_present(df: pd.DataFrame, col_name: Any, df_name: str) -> None:
    """
    Raises an error if a pandas series has missing values.

    Args:
        df : Pandas DataFrame
    """
    if df[col_name].isnull().any():
        raise ValueError(
            f"Missing values are present in the {df_name} dataframe's {col_name} column")


@typechecked
def _is_zero(f: float) -> bool:
    return math.isclose(f, 0, abs_tol=0.01)


@typechecked
def _is_one(f: float) -> bool:
    return math.isclose(f, 1, abs_tol=0.01)


@typechecked
def _num_non_binary_values(s: pd.Series) -> int:
    """
    Returns the number of values in a series that are neither 0, 1, nor missing.
    """
    non_binary_vals = (~s.dropna().isin([0, 1])).sum()
    return int(non_binary_vals)


def _check_binary(df: pd.DataFrame) -> None:
    """
    Warns if any column of a pandas DataFrame is not a binary value equal to 0 or 1.

    Args:
        df : Pandas DataFrame
    """
    for label in df:
        num_non_binary_vals = _num_non_binary_values(df[label])
        if num_non_binary_vals != 0:
            warnings.warn(
                f"Column {label} is not binary. It has {num_non_binary_vals} non-binary value(s).",
                UserWarning)


@typechecked
def _is_binary(df: pd.DataFrame) -> bool:
    """
    Checks whether a Pandas DataFrame is all binary.

    Args:
        df : Pandas DataFrame
    """
    return df.isin([0, 1, None]).all(axis=None).item()


@typechecked
def _fill_na_and_standardize(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces the NaNs in a Pandas DataFrame with the mean of the column and
    mean centers and scales the columns.

    Args:
        df : Pandas DataFrame
    """
    pdf_mean = pdf.mean()
    _pdf = pdf.fillna(pdf_mean)
    pdf_std = pdf.std()
    zero_std_labels = _pdf.columns[pdf_std == 0]
    if zero_std_labels.empty:
        return (_pdf - pdf_mean) / pdf_std
    else:
        raise ValueError(f"""Column(s) {zero_std_labels.tolist()} have zero std. dev.!""")


@typechecked
def _prepare_labels_and_warn(label_df: pd.DataFrame, is_binary: bool,
                             label_type: str) -> pd.DataFrame:
    """
    Gets a Pandas DataFrame containing phenotypes (label_df) and whether it is binary or not
    and generates the appropriate warning based on label_type. Returns standardized label DataFrame

    Args:
        label_df: Pandas DataFrame of phenotypes
        is_binary: Whether label_df is binary or not
        label_type: User's choice for label type treatment ('detect', 'binary', or 'quantitative')

    Returns:
        Standardized label Pandas Dataframe
    """
    if label_type == 'detect':
        if is_binary:
            print(
                "The label DataFrame is binary. Reduction/regression for binary phenotypes will be applied.",
            )
        else:
            print(
                "The label DataFrame is quantitative. Reduction/regression for quantitative phenotypes will be applied.",
            )
        return _fill_na_and_standardize(label_df)
    elif label_type == 'quantitative':
        print("Reduction/regression for quantitative phenotypes will be applied.")
        return _fill_na_and_standardize(label_df)
    elif label_type == 'binary':
        if is_binary:
            print("Reduction/regression for binary phenotypes will be applied.")
            return _fill_na_and_standardize(label_df)
        else:
            _check_binary(label_df)
            raise TypeError("Binary label DataFrame expected!", UserWarning)
    else:
        raise ValueError(
            f'label_type should be "quantitative", "binary", or "detect". Found {label_type}.')


@typechecked
def _prepare_covariates(cov_df: pd.DataFrame, label_df: pd.DataFrame,
                        add_intercept: bool) -> pd.DataFrame:
    """
    Standardizes the covariate Pandas DataFrame and adds intercept column to it if indicated

    Args:
        cov_df: Pandas DataFrame of phenotypes
        label_df: Pandas DataFrame of phenotypes
        add_intercept: Whether to add intercept to the covariates Pandas DataFrame

    Returns:
        Standardized covariate Pandas DataFrame with optionally added intercept column
    """
    if cov_df.empty:
        if add_intercept:
            std_cov_df = pd.DataFrame(data=np.ones(label_df.shape[0]),
                                      columns=['intercept'],
                                      index=label_df.index)
        else:
            std_cov_df = cov_df
    else:
        if label_df.shape[0] != cov_df.shape[0]:
            raise ValueError(
                f'cov_df must be either empty of have the same number of rows as label_df ({label_df.shape[0]} != {cov_df.shape[0]}'
            )
        std_cov_df = _fill_na_and_standardize(cov_df)
        if add_intercept:
            std_cov_df.insert(0, 'intercept', 1)

    return std_cov_df


@typechecked()
def _check_model(model_df: DataFrame) -> None:
    """
    Raise an error if model_df does not exist

    Args:
        model_df : Model DataFrame
    """
    if model_df is None:
        raise ValueError(
            'No model DataFrame found! Run fit() or provide a previously made model using set_model_df()'
        )


@typechecked()
def _check_cv(cv_df: DataFrame) -> None:
    """
    Raise an error if cv_df does not exist

    Args:
        cv_df: Cross Validation DataFrame
    """
    if cv_df is None:
        raise ValueError(
            'No cross validation DataFrame found! Run fit() or provide a previously made cv DataFrame using set_cv_df().'
        )


@typechecked
def flatten_prediction_df(blockdf: DataFrame, sample_blocks: Dict[str, Iterable[str]],
                          labeldf: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a Spark DataFrame containing a block matrix of model predictions to a flat Pandas DataFrame with the same
    shape as labeldf.

    Args:
        blockdf : Spark DataFrame containing a block matrix of predictions for the labels in labeldf
        sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
        labeldf : Pandas DataFrame containing target labels

    Returns:
        Pandas DataFrame containing prediction values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label
    """
    sample_block_df = blockdf.sql_ctx \
        .createDataFrame(sample_blocks.items(), ['sample_block', 'sample_ids']) \
        .selectExpr('sample_block', 'posexplode(sample_ids) as (idx, sample_id)')

    flattened_prediction_df = blockdf \
        .selectExpr('sample_block', 'label', 'posexplode(values) as (idx, value)') \
        .join(sample_block_df, ['sample_block', 'idx'], 'inner') \
        .select('sample_id', 'label', 'value')

    pivoted_df = flattened_prediction_df.toPandas() \
        .pivot(index='sample_id', columns='label', values='value') \
        .reindex(index=labeldf.index, columns=labeldf.columns)

    return pivoted_df


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


def cross_validation(blockdf, modeldf, score_udf, score_key_pattern, alphas, metric):
    """
    Performs cross-validated optimization over the shrinkage hyperparameters alpha, using the models in modeldf and
    the block feature matrix blockdf.

    Args:
        blockdf : Spark DataFrame representing a once- or twice-reduced block matrix.
        modeldf : Spark DataFrame produced by the LogisticRegression or RidgeRegression fit method, representing the
        reducer models fit using the shrinkage parameters alpha.
        score_udf : Pandas UDF used to apply the scoring function to blockdf and modeldf
        score_key_pattern : Pattern of column names used in the groupBy clause before applying score_udf
        alphas : dict of alpha_name : alpha_value
        metric : string specifying either the r2 or the log_loss metric for model scoring

    Returns:
        Spark DataFrame  containing the results of the cross validation procedure.
    """
    alpha_df = blockdf.sql_ctx \
        .createDataFrame([Row(alpha=k, alpha_value=float(v)) for k, v in alphas.items()])
    if metric == 'r2':
        window_spec = Window.partitionBy('label').orderBy(f.desc('r2_mean'), f.desc('alpha_value'))
    elif metric == 'log_loss':
        window_spec = Window.partitionBy('label').orderBy('log_loss_mean', f.desc('alpha_value'))
    else:
        raise ValueError(f'Metric should be either "r2" or "log_loss", found {metric}')

    # Hint a sort-merge join to avoid an automatic broadcast join that may cause an OOM
    return blockdf.drop('header_block', 'sort_key') \
        .join(modeldf.hint('merge'), ['header', 'sample_block'], 'right') \
        .withColumn('label', f.coalesce(f.col('label'), f.col('labels').getItem(0))) \
        .groupBy(score_key_pattern) \
        .apply(score_udf) \
        .join(alpha_df, ['alpha']) \
        .groupBy('label', 'alpha', 'alpha_value').agg(f.mean('score').alias(f'{metric}_mean')) \
        .withColumn('modelRank', f.row_number().over(window_spec)) \
        .filter('modelRank = 1') \
        .drop('modelRank')


def apply_model_df(blockdf, modeldf, cvdf, transform_udf, transform_key_pattern, join_type):
    """
    Applies a model to a reduced block matrix and returns the predictions.

    Args:
        blockdf : Spark DataFrame representing a once- or twice-reduced block matrix.
        modeldf : Spark DataFrame produced by the LogisticRegression or RidgeRegression fit method, representing the
        reducer models fit using the shrinkage parameters alpha.
        cvdf : Spark DataFrame returning the cross validation results
        transform_udf : Pandas UDF used to apply the model
        transform_key_pattern : Pattern of column names used in the groupBy clause
        join_type : Join type for join between blockdf and modeldf. 
    """

    # Hint a sort-merge join to avoid an automatic broadcast join that may cause an OOM
    return blockdf.drop('header_block', 'sort_key') \
        .join(modeldf.drop('header_block').hint('merge'), ['sample_block', 'header'], join_type) \
        .withColumn('label', f.coalesce(f.col('label'), f.col('labels').getItem(0))) \
        .groupBy(transform_key_pattern) \
        .apply(transform_udf) \
        .join(cvdf, ['label', 'alpha'], 'inner')
