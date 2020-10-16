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

from glow import glow
import hail as hl
from hail import MatrixTable
from hail.expr.types import tarray, tbool, tcall, tlocus, tstr, tstruct
from hail.methods import misc
from pyspark.sql import DataFrame
from typeguard import check_argument_types, check_return_type

aliases = {
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


def from_matrix_table(mt: MatrixTable, include_sample_ids=True) -> DataFrame:
    """
    Converts a Hail MatrixTable to a Glow DataFrame.

    Args:
        mt : The Hail MatrixTable to convert

    Returns:
        Glow DataFrame converted from the MatrixTable.
    """

    assert check_argument_types()

    misc.require_row_key_variant_w_struct_locus(mt, 'glow.hail.from_matrix_table')

    # Genotypes column
    has_sample_ids = isinstance(mt.col_key.dtype,
                                tstruct) and 's' in mt.col_key and mt.col_key.s.dtype == tstr
    named_struct_args = []
    for entry in mt.entry:
        if entry == 'GT' and mt.entry.GT.dtype == tcall:
            # Flatten GT into non-null calls and phased
            named_struct_args.append(
                "'calls', nvl(e.GT.alleles, array(-1, -1)), 'phased', nvl(e.GT.phased, false)")
        elif entry in aliases:
            # Rename aliased genotype fields
            named_struct_args.append(f"'{aliases[entry_field]}', e.{entry_field}")
        else:
            # Rename genotype fields
            named_struct_args.append(f"'{entry_field}', e.{entry_field}")
    named_struct_expr_args = ' ,'.join(named_struct_args)
    if include_sample_ids and has_sample_ids:
        sample_id_expr_args = ' ,'.join([f"'{s}'" for s in mt.col_key.s.collect()])
        sample_id_expr = f"array({sample_id_expr_args})"
        genotypes_col = fx.expr(
            f"zip_with({sample_id_expr}, entries, (s, e) -> named_struct('sampleId', s, {named_struct_expr_args}))"
        )
    else:
        genotypes_col = fx.expr(f"transform(entries, e -> named_struct({named_struct_expr_args}))")

    # Base columns
    has_info_struct = 'info' in mt.rows().row and isinstance(mt.rows().row.info.dtype, tstruct)

    start_col = fx.col("`locus.position`") - 1

    end_col = start_col + fx.length(fx.element_at("alleles", 1))
    if has_info_struct and 'END' in mt.rows().row.info:
        end_col = fx.coalesce(fx.col("`info.END`"), end_col)

    names_cols = []
    if 'varid' in mt.rows().row:
        names_cols.append("varid")
    if 'rsid' in mt.rows().row:
        names_cols.append("rsid")
    names_col = fx.expr(f"filter(array({','.join(names_cols)}), n -> isnotnull(n))")

    other_cols = []
    if 'cm_position' in mt.rows().row:
        other_cols.append(fx.col("cm_position").alias("position"))
    if 'qual' in mt.rows().row:
        # -10 qual means missing
        other_cols.append(fx.expr("if(qual = -10, null, qual)").alias("qual"))
    # null filters means missing, [] filters means PASS
    if 'filters' in mt.rows().row:
        other_cols.append(
            fx.expr("if(size(filters) = 0, array('PASS'), if(isnull(filters), array(), filters))").
            alias("filters"))
    # Rename info.* columns to INFO_*
    if 'info' in mt.rows().row:
        for f in mt.rows().row.info:
            if mt.rows().row.info[f].dtype == tbool:
                # FLAG INFO fields should be null if not present; Hail encodes them as false
                other_cols.append(
                    fx.expr(f"if(`info.{f}` = false, null, `info.{f}`)").alias(f"INFO_{f}"))
            else:
                other_cols.append(fx.col(f"`info.{f}`").alias(f"INFO_{f}"))

    glow_compatible_df = mt.localize_entries('entries').to_spark().select(
        fx.col("`locus.contig`").alias("contigName"),
        start_col.cast("long").alias("start"),
        end_col.cast("long").alias("end"), names_col.alias("names"),
        fx.element_at("alleles", 1).alias("referenceAllele"),
        fx.expr("filter(alleles, (a, i) -> i > 0)").alias("alternateAlleles"), *other_cols,
        genotypes_col.alias("genotypes"))

    assert check_return_type(glow_compatible_df)
    return glow_compatible_df
