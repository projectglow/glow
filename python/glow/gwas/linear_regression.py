import pandas as pd
import numpy as np
import numpy.ma as ma
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from glow.functions import *
import opt_einsum as oe
from scipy import stats

__all__ = ['linear_regression']

def linear_regression(genotype_df, phenotype_df, covariate_df, fit_intercept=True, values_field='values'):
    result_fields = [
        StructField('effect', DoubleType()),
        StructField('standard_error', DoubleType()),
        StructField('tvalue', DoubleType()),
        StructField('pvalue', DoubleType()),
        StructField('phenotype', StringType())
    ]
    fields = [field for field in genotype_df.schema.fields if field.name != values_field] + result_fields
    result_struct = StructType(fields)
    C = covariate_df.to_numpy('float64', copy=True)
    if fit_intercept:
        intercept = np.ones((phenotype_df.shape[0], 1))
        C = np.hstack((intercept, C)) if C.size else intercept
    
    Y = phenotype_df.to_numpy('float64', copy=True)
    Y_mask = ~np.isnan(Y)
    Y[~Y_mask] = 0
    Q = np.zeros((covariate_df.shape[0], covariate_df.shape[1], phenotype_df.shape[1]))
    Q = np.linalg.qr(C)[0]
    QtY = Q.T @ Y
    YdotY = np.sum(Y * Y, axis = 0) - np.sum(QtY * QtY, axis = 0)
    dof = C.shape[0] - C.shape[1] - 1

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield _linear_regression_inner(pdf, Y, Q, QtY, YdotY, Y_mask, dof, phenotype_df.columns.to_series().astype('str'))

    return genotype_df.mapInPandas(map_func, result_struct)


def _add_intercept(C, num_samples):
    intercept = np.ones((num_samples, 1))
    return np.hstack((intercept, C)) if C.size else intercept


def _einsum(subscripts, *operands):
    '''
    A wrapper around opt_einsum to ensure uniform memory limits.
    '''
    return oe.contract(subscripts, *operands, memory_limit=5e7)


def _linear_regression_inner(genotype_df, Y, Q, QtY, YdotY, Y_mask, dof, phenotype_names):
    '''
    Applies a linear regression model to a block of genotypes.
    This is effectively a batch version of core/src/main/scala/io/projectglow/sql/expressions/LinearRegressionGwas.scala

    To account for samples with missing traits, we additionally accept a mask indicating which samples are missing
    for each phenotype. This mask is used to ensure that missing samples are not included when summing across individuals.

    Rather than use traditional matrix indices in the einsum expressions, we use semantic indices.
    s: sample
    g: genotype
    p: phenotype
    c: covariate

    So, if a matrix's indices are `sg` (like the X matrix), it has one row per sample and one column per genotype.
    '''
    X = np.column_stack(genotype_df['values'].array)

    with oe.sharing.shared_intermediates():
        XdotY = Y.T @ X - _einsum('cp,sc,sg,sp->pg', QtY, Q, X, Y_mask)
        XdotY = Y.T @ X - _einsum('cp,sc,sg,sp->pg', QtY, Q, X, Y_mask)
        QtX = _einsum('sc,sp,sg->pgc', Q, Y_mask, X)
        XdotX_reciprocal = 1 / (_einsum('sp,sg,sg->pg', Y_mask, X, X) -
                                _einsum('pgc,pgc->pg', QtX, QtX))

    betas = XdotY * XdotX_reciprocal
    QtX = _einsum('sc,sp,sg->pgc', Q, Y_mask, X)
    standard_error = np.sqrt((YdotY[:, None] * XdotX_reciprocal - betas * betas) / dof)
    T = betas / standard_error
    pvalues = 2 * stats.distributions.t.sf(np.abs(T), dof)

    del genotype_df['values']
    out_df = pd.concat([genotype_df] * Y.shape[1])
    out_df['effect'] = list(np.ravel(betas))
    out_df['standard_error'] = list(np.ravel(standard_error))
    out_df['tvalue'] = list(np.ravel(T))
    out_df['pvalue'] = list(np.ravel(pvalues))
    out_df['phenotype'] = phenotype_names.repeat(genotype_df.shape[0]).tolist()

    return out_df
