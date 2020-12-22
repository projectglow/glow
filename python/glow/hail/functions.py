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

import hail as hl
from hail import MatrixTable
from hail.expr.expressions.typed_expressions import StructExpression
from hail.expr.types import tarray, tbool, tcall, tfloat64, tint, tlocus, tset, tstr, tstruct
from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as fx
from typing import List, NoReturn, Optional
from typeguard import check_argument_types, check_return_type

__all__ = ['from_matrix_table']


def _get_sample_ids(col_key: StructExpression, include_sample_ids: bool) -> Optional[List[str]]:
    assert check_argument_types()

    has_sample_ids = 's' in col_key and col_key.s.dtype == tstr
    if include_sample_ids and has_sample_ids:
        sample_ids = [f"'{s}'" for s in col_key.s.collect()]
    else:
        sample_ids = None

    assert check_return_type(sample_ids)
    return sample_ids


def _get_genotypes_col(entry: StructExpression, sample_ids: Optional[List[str]]) -> Column:
    assert check_argument_types()

    entry_aliases = {
        'DP': 'depth',
        'FT': 'filters',
        'GL': 'genotypeLikelihoods',
        'PL': 'phredLikelihoods',
        'GP': 'posteriorProbabilities',
        'GQ': 'conditionalQuality',
        'HQ': 'haplotypeQualities',
        'EC': 'expectedAlleleCounts',
        'MQ': 'mappingQuality',
        'AD': 'alleleDepths'
    }

    base_struct_args = []
    for entry_field in entry:
        if entry_field == 'GT' and entry.GT.dtype == tcall:
            # Flatten GT into calls and phased
            base_struct_args.append("'calls', e.GT.alleles, 'phased', e.GT.phased")
        elif entry[entry_field].dtype == tcall:
            # Turn other call fields (eg. PGT) into a string
            base_struct_args.append(
                f"'{entry_field}', array_join(e.{entry_field}.alleles, if(e.{entry_field}.phased, '|', '/'))"
            )
        elif entry_field in entry_aliases:
            # Rename aliased genotype fields
            base_struct_args.append(f"'{entry_aliases[entry_field]}', e.{entry_field}")
        else:
            # Rename genotype fields
            base_struct_args.append(f"'{entry_field}', e.{entry_field}")

    if sample_ids is not None:
        sample_id_expr = f"array({' ,'.join(sample_ids)})"
        struct_expr = ' ,'.join(["'sampleId', s"] + base_struct_args)
        genotypes_col = fx.expr(
            f"zip_with({sample_id_expr}, entries, (s, e) -> named_struct({struct_expr}))")
    else:
        struct_expr = ' ,'.join(base_struct_args)
        genotypes_col = fx.expr(f"transform(entries, e -> named_struct({struct_expr}))")
    genotypes_col = genotypes_col.alias("genotypes")

    assert check_return_type(genotypes_col)
    return genotypes_col


def _get_base_cols(row: StructExpression) -> List[Column]:
    assert check_argument_types()

    contig_name_col = fx.col("`locus.contig`").alias("contigName")

    start_col = (fx.col("`locus.position`") - 1).cast("long").alias("start")

    end_col = start_col + fx.length(fx.element_at("alleles", 1))
    has_info = 'info' in row and isinstance(row.info.dtype, tstruct)
    if has_info and 'END' in row.info and row.info.END.dtype == tint:
        end_col = fx.coalesce(fx.col("`info.END`"), end_col)
    end_col = end_col.cast("long").alias("end")

    names_elems = []
    if 'varid' in row and row.varid.dtype == tstr:
        names_elems.append("varid")
    if 'rsid' in row and row.rsid.dtype == tstr:
        names_elems.append("rsid")
    names_col = fx.expr(
        f"nullif(filter(array({','.join(names_elems)}), n -> isnotnull(n)), array())").alias("names")

    reference_allele_col = fx.element_at("alleles", 1).alias("referenceAllele")

    alternate_alleles_col = fx.expr("slice(alleles, 2, size(alleles) - 1)").alias("alternateAlleles")

    base_cols = [
        contig_name_col, start_col, end_col, names_col, reference_allele_col, alternate_alleles_col
    ]
    assert check_return_type(base_cols)
    return base_cols


def _get_other_cols(row: StructExpression) -> List[Column]:
    assert check_argument_types()

    other_cols = []
    if 'cm_position' in row and row.cm_position.dtype == tfloat64:
        other_cols.append(fx.col("cm_position").alias("position"))
    if 'qual' in row and row.qual.dtype == tfloat64:
        # -10 qual means missing
        other_cols.append(fx.expr("if(qual = -10, null, qual)").alias("qual"))
    # [] filters means PASS, null filters means missing
    if 'filters' in row and row.filters.dtype == tset(tstr):
        other_cols.append(fx.expr("if(size(filters) = 0, array('PASS'), filters)").alias("filters"))
    # Rename info.* columns to INFO_*
    if 'info' in row and isinstance(row.info.dtype, tstruct):
        for f in row.info:
            other_cols.append(fx.col(f"`info.{f}`").alias(f"INFO_{f}"))

    assert check_return_type(other_cols)
    return other_cols


def _require_row_variant_w_struct_locus(mt: MatrixTable) -> NoReturn:
    """
    Similar to hail.methods.misc.require_row_key_variant_w_struct_locus, but not necessarily as keys
    """
    assert check_argument_types()

    if (not set(['locus', 'alleles']).issubset(set(mt.rows().row)) or
            not mt['alleles'].dtype == tarray(tstr) or
        (not isinstance(mt['locus'].dtype, tlocus) and
         mt['locus'].dtype != hl.dtype('struct{contig: str, position: int32}'))):
        raise ValueError("'hail.from_matrix_table' requires row to contain two fields 'locus'"
                         " (type 'locus<any>' or 'struct{{contig: str, position: int32}}') and "
                         "'alleles' (type 'array<str>')")


def from_matrix_table(mt: MatrixTable, include_sample_ids: bool = True) -> DataFrame:
    """
    Converts a Hail MatrixTable to a Glow DataFrame. The variant fields are derived from the Hail MatrixTable
    row fields. The sample IDs are derived from the Hail MatrixTable column fields. All other genotype fields are
    derived from the Hail MatrixTable entry fields.

    Requires that the MatrixTable rows contain locus and alleles fields.

    Args:
        mt : The Hail MatrixTable to convert
        include_sample_ids : If true (default), include sample IDs in the Glow DataFrame.
                             Sample names increase the size of each row, both in memory and on storage.

    Returns:
        Glow DataFrame converted from the MatrixTable.
    """

    assert check_argument_types()

    # Ensure that dataset rows contain locus and alleles
    _require_row_variant_w_struct_locus(mt)

    glow_compatible_df = mt.localize_entries('entries').to_spark().select(
        *_get_base_cols(mt.rows().row), *_get_other_cols(mt.rows().row),
        _get_genotypes_col(mt.entry, _get_sample_ids(mt.col_key, include_sample_ids)))

    assert check_return_type(glow_compatible_df)
    return glow_compatible_df
