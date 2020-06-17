.. _merge-datasets:

========================
Merging Variant Datasets
========================

.. invisible-code-block: python

    import glow
    glow.register(spark)
    path1 = 'test-data/vcf-merge/HG00096.vcf.bgz'
    path2 = 'test-data/vcf-merge/HG00097.vcf.bgz'

You can use Glow and Spark to merge genomic variant datasets from non-overlapping sample sets into
a multi-sample dataset. In these examples, we will read from VCF files, but the same logic works
on :ref:`DataFrames backed by other file formats <variant_data>`.

First, read the VCF files into a single Spark DataFrame:

.. code-block:: python

  from pyspark.sql.functions import *

  df = spark.read.format('vcf').load([path1, path2])

  # Alternatively, you can use the "union" DataFrame method if the VCF files have the same schema
  df1 = spark.read.format('vcf').load(path1)
  df2 = spark.read.format('vcf').load(path2)
  df = df1.union(df2)

The resulting DataFrame contains all records from the VCFs you want to merge, but the genotypes from
different samples at the same site have not been combined. You can use an aggregation to combine the
genotype arrays.

.. code-block:: python

  from pyspark.sql.functions import *

  merged_df = df.groupBy('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles')\
    .agg(sort_array(flatten(collect_list('genotypes'))).alias('genotypes'))

.. invisible-code-block: python

  from pyspark.sql import Row

  row = merged_df.orderBy('contigName', 'start').select('contigName', 'start', 'genotypes.sampleId').head()
  assert_rows_equal(row, Row(contigName='22', start=16050074, sampleId=['HG00096', 'HG00097']))

.. important::
  
  When reading VCF files for a merge operation, ``sampleId`` must be the first field in the
  genotype struct. This is the default Glow schema.

The genotypes from different samples now appear in the same genotypes array.

.. note::
  
  If the VCFs you are merging contain different sites, elements will be missing from the genotypes
  array after aggregation. Glow automatically fills in missing genotypes when writing to
  ``bigvcf``, so an exported VCF will still contain all samples.

Aggregating INFO fields
-----------------------

To preserve INFO fields in a merge, you can use the aggregation functions in Spark. For instance, to
emit an ``INFO_DP`` column that is the sum of the ``INFO_DP`` columns across all samples:

.. code-block:: python

  from pyspark.sql.functions import *

  merged_df = df.groupBy('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles')\
    .agg(sort_array(flatten(collect_list('genotypes'))).alias('genotypes'),
         sum('INFO_DP').alias('INFO_DP'))

.. invisible-code-block: python

  row = merged_df.orderBy('contigName', 'start').select('contigName', 'start', 'genotypes.sampleId', 'INFO_DP').head()
  assert_rows_equal(row, Row(contigName='22', start=16050074, sampleId=['HG00096', 'HG00097'],
    INFO_DP=16024))
  

Joint genotyping
----------------

The merge logic in this document allows you to quickly aggregate genotyping array data or single
sample VCFs. For a more sophisticated aggregation that unifies alleles at overlapping sites and uses
cohort-level statistics to refine genotype calls, we recommend running a joint genotyping pipeline
like `the one included in the Databricks Runtime for Genomics
<https://docs.databricks.com/applications/genomics/tertiary/joint-genotyping-pipeline.html>`_.

.. notebook:: .. etl/merge-vcf.html
