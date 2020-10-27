import pandas as pd
import numpy as np
import numpy.ma as ma
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from glow.functions import *
import opt_einsum as oe

__all__ = ['linear_regression']

def linear_regression(genotype_df, phenotype_df, covariate_df, fit_intercept=True, values_field='values'):
    result_fields = [
        StructField('tvalue', DoubleType()),
        StructField('pvalue', DoubleType()),
        StructField('effect', DoubleType())
    ]
    fields = [field for field in genotype_df.schema.fields if field.name != values_field] + result_fields
    result_struct = StructType(fields)
    C = covariate_df.to_numpy('float64', copy=True)
    if fit_intercept:
        C = np.hstack((np.ones(C.shape[0]), C))
    Q = np.linalg.qr(C)[0]
    Y = phenotype_df.to_numpy('float64', copy=True)
    Y_mask = ~np.isnan(Y)
    Y[~Y_mask] = 0
    QtY = Q.T @ Y
    YdotY = np.sum(Y * Y, axis = 0) - np.sum(QtY * QtY, axis = 0)
    dof = c.shape[0] - c.shape[1] - 1

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield _linear_regression_inner(pdf, Y, Q, QtY, YdotY, Y_mask, dof)

    return genotype_df.mapInPandas(map_func, result_struct)


def _einsum(subscripts, *operands):
    '''
    A wrapper around opt_einsum to ensure uniform memory limits.
    '''
    return oe.contract(subscripts, *operands, memory_limit=5e7)


def _linear_regression_inner(genotype_df, Y, Q, QtY, YdotY, Y_mask, dof):
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
        XdotY = Y.T @ X - einsum('cp,sc,sg,sp->pg', QtY, Q, X, Y_mask)
        XdotX_reciprocal = 1 / (einsum('sp,sg,sg->pg', Y_mask, X, X) -
                                einsum('sc,sp,sg,sc,sp,sg->pg', Q, Y_mask, X, Q, Y_mask, X))

    betas = XdotY * XdotX_reciprocal
    standard_error = np.sqrt((YdotY[:, None] * XdotX_reciprocal - betas * betas) / dof)
    T = betas / standard_error
    pvalues = 2 * stats.distributions.t.sf(np.abs(T), dof)

    del genotype_df['values']
    out_df = pd.concat([genotype_df] * Y.shape[1])
    out_df['effect'] = list(np.ravel(betas))
    out_df['tvalue'] = list(np.ravel(T))
    out_df['pvalue'] = list(np.ravel(pvalues))

    return out_df
