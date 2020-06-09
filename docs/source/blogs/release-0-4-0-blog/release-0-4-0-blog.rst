===================================================================
Glow 0.4 Enables Integration of Genomic Variant and Annotation Data
===================================================================

| Author: `Kiavash Kianfar <https://github.com/kianfar77>`_
| June 2, 2020

.. _`GFF3`: https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md
Glow 0.4 was released on May 20, 2020. This blog is mainly focused on the highlight of this release, which is the introduction of genomic annotation data ingest capability from the `GFF3 (Generic Feature Format Version 3) <https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md>`_ flat file format. This release includes some other feature and usability improvements, which we will mention at the end.

`GFF3`_ is a sequence annotation flat file format proposed by the `Sequence Ontology Project <http://www.sequenceontology.org/>`_ in 2013, which since has become the de facto format for genome annotation and is widely used by genome browsers and databases such as NCBI `RefSeq <https://www.ncbi.nlm.nih.gov/refseq/>`_ and `GenBank <https://www.ncbi.nlm.nih.gov/genbank/>`_. `GFF3`_, a 9-column tab-separated text format, typically carries the majority of annotation data in the ninth column, called ``attributes``, as a semi-colon-separated list of ``<tag>=<value>`` entries. As a result, although `GFF3`_ files can be read as Spark DataFrames using Spark SQL's standard ``csv`` data source, the resulting DataFrame's schema is not very useful for query and data manipulation of annotation data as the whole list of attribute tag-value pairs for each sequence will appear as a single semi-colon-separated string in the `attributes` column of the DataFrame.

Glow 0.4.0 has been equipped with the new and flexible ``gff`` Spark SQL data source to address this challenge and create a smooth GFF3 ingest and query experience. While reading the GFF3  file, the ``gff`` data source parses the ``attributes`` column of the file to create an appropriately typed column for each tag, which, for each row, will contain the value corresponding to that tag in that row (or ``null`` if the tag does not appear in the row). Consequently, all tags in the GFF3 ``attributes`` column will have their own corresponding column in the Spark DataFrame, making annotation data query and manipulation much easier.

.. _gff3_ingest:

Ingest GFF3 Annotation Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Like any other Spark data source, reading GFF3 files using Glow's ``gff`` data source can be done in a single line of code. As an example, the sequence annotation data for the Homo Sapiens genome assembly GRCh38.p13, which we have obtained from `RefSeq ftp site <https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/reference/GCF_000001405.39_GRCh38.p13/>`_ in the GFF3 format, can be ingested using the ``gff`` reader as shown below, resulting in the ``annotations_df`` DataFrame shown in :numref:`fig_annotations_df`.

.. _annotations_df:

.. code-block::

  import glow
  glow.register(spark)

  gff_path = '/databricks-datasets/genomics/gffs/GCF_000001405.39_GRCh38.p13_genomic.gff.bgz'

  annotations_df = spark.read.format("gff").load(gff_path) \
      .filter("seqid = 'NC_000022.11'") \
      .alias('annotations_df')

.. figure:: annotations_df.png
   :align: center
   :width: 800
   :name: fig_annotations_df

   A small section of the ``annotations_df`` DataFrame

Here, we have additionally filtered the results to chromosome 22 and given an alias to the resulting DataFrame to use it later in continuation of this example.

In addition to reading uncompressed ``.gff`` files, the ``gff`` data source supports all compression formats supported by Spark's ``csv`` data source, including ``.gz`` and ``.bgz``. It is strongly recommended to use splittable compression formats like ``.bgz`` instead of ``.gz`` for better parallelization of the read process.

Schema
~~~~~~
Let us have a closer look at the schema of the resulting DataFrame, which was automatically inferred by  Glow's ``gff`` data source:

.. code-block::

  annotations_df.printSchema()

.. code-block::

    root
     |-- seqId: string (nullable = true)
     |-- source: string (nullable = true)
     |-- type: string (nullable = true)
     |-- start: long (nullable = true)
     |-- end: long (nullable = true)
     |-- score: double (nullable = true)
     |-- strand: string (nullable = true)
     |-- phase: integer (nullable = true)
     |-- ID: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Parent: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- Target: string (nullable = true)
     |-- Gap: string (nullable = true)
     |-- Note: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- Dbxref: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- Is_circular: boolean (nullable = true)
     |-- align_id: string (nullable = true)
     |-- allele: string (nullable = true)
     .
     .
     .
     |-- transl_table: string (nullable = true)
     |-- weighted_identity: string (nullable = true)

This schema has 100 fields (not all shown here). The first eight fields (``seqId``, ``source``, ``type``, ``start``, ``end``, ``score``, ``strand``, and ``phase``), referred to as "base" fields, correspond to the first eight columns of the `GFF3`_ format in proper data type. The rest of the fields in the inferred schema are the result of parsing the ``attributes`` column of the GFF3 file. Fields corresponding to any "official" tag that appears in the GFF3 file come first in appropriate data types. Official fields are those referred to as `tags with pre-defined meaning <https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md>`_ in the `GFF3`_ format description. The official fields are followed by "unofficial" fields (fields corresponding to any other tag) in the alphabetical order. In the example above, the fields ``ID``, ``Name``, ``Parent``, ``Target``, ``Gap``, ``Note``, ``Dbxref``, and ``Is_circular`` are the official, and the rest of the fields are unofficial fields. Note that the ``gff`` data source discards the comments, directives, and FASTA lines that may be in the GFF3 file.

As the official tags do not have a unique spelling across different GFF3 files in terms of letter case and underscore usage, the ``gff`` data source is designed to be insensitive to letter case and underscore in extracting official tags from the ``attributes`` field. For example, the official tag ``Dbxref`` will be correctly extracted as an official field even if it appears as ``dbxref`` or ``dbx_ref`` in the GFF3 file. Note that the field name spelling in the resulted DataFrame will be exactly as it is in the GFF3 file (see `Glow documentation <https://glow.readthedocs.io/en/latest/etl/gff.html>`_ for more details).

Like other Spark data sources, Glow's ``gff`` data source is also able to accept a user-specified schema through the ``.schema`` command. The data source behavior in this case is also designed to be quite flexible. More specifically, the fields (and their types) in the user-specified schema are treated as the list of fields (whether base, official, or unofficial) to be extracted from the GFF3 file (and cast to the specified types). Please refer to `Glow documentation <https://glow.readthedocs.io/en/latest/etl/gff.html>`_ for more details in how user-specified schemas can be used.

Example: Gene Transcripts and Transcript Exons
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
With the annotation tags extracted as individual DataFrame columns using Glow's ``gff`` data source, query and data preparation over genetic annotations becomes as easy as writing common Spark SQL commands in the user's API of choice. As an example, here we demonstrate how simple queries can be used to extract data regarding hierarchical grouping of genomic features from the ``annotations_df`` created :ref:`above <annotations_df>`.

One of the main advantages of `GFF3`_ format compared to older versions of GFF is the improved presentation of feature hierarchies. (see  `GFF3`_ format description for more details). Two examples of such hierarchies are:

- Transcripts of a gene. Here, the gene is the "parent" and its transcripts are the "children".
- Exons of a transcript. Here, the transcript is a parent feature and its exons are the children.

In the `GFF3`_ format, the value of the ``parent`` tag in the ``attributes`` column indicates all parent ID(s) of each row. This value is extracted as an array of parent ID(s) in the ``parent`` column of the DataFrame created by the ``gff`` reader .

Assume we would like to create a DataFrame, called ``gene_transcript_df``, which, for each gene on chromosome 22, provides some basic information about the gene and all its transcripts.  As each row in  the ``annotations_df`` of our example has at most a single parent, the ``parent_child_df`` created by the following query will help us in achieving our goal. This query joins ``annotations_df`` with a subset of its own columns on the ``parent`` column as the key. :numref:`fig_parent_child_df` shows a small section of the ``parent_child_df``.

.. code-block::

    from pyspark.sql.functions import *

    parent_child_df = annotations_df \
    .join(
      annotations_df.select('id', 'type', 'name', 'start', 'end').alias('parent_df'),
      col('annotations_df.parent')[0] == col('parent_df.id') # each row in annotation_df has at most one parent
    ) \
    .orderBy('annotations_df.start', 'annotations_df.end') \
    .select(
      'annotations_df.seqid',
      'annotations_df.type',
      'annotations_df.start',
      'annotations_df.end',
      'annotations_df.id',
      'annotations_df.name',
      col('annotations_df.parent')[0].alias('parent_id'),
      col('parent_df.Name').alias('parent_name'),
      col('parent_df.type').alias('parent_type'),
      col('parent_df.start').alias('parent_start'),
      col('parent_df.end').alias('parent_end')
    ) \
    .alias('parent_child_df')


.. figure:: parent_child_df.png
   :align: center
   :width: 800
   :name: fig_parent_child_df

   A small section of the ``parent_child_df`` DataFrame


Having the ``parent_child_df`` DataFrame, we can now write the following simple function, called ``parent_child_summary``, which, given the parent and child types, generates a DataFrame containing basic information on each parent of the given type and all its children of the given type.

.. code-block::

    def parent_child_summary(parent: str, child: str) -> DataFrame:
      return parent_child_df \
        .select(
          'seqid',
          col('parent_id').alias(f'{parent}_id'),
          col('parent_name').alias(f'{parent}_name'),
          col('parent_start').alias(f'{parent}_start'),
          col('parent_end').alias(f'{parent}_end'),
          col('id').alias(f'{child}_id'),
          col('start').alias(f'{child}_start'),
          col('end').alias(f'{child}_end'),
        ) \
        .where(f"type == '{child}' and parent_type == '{parent}'") \
        .orderBy(
          'seqid',
          f'{parent}_start',
          f'{parent}_end',
          f'{child}_start',
          f'{child}_end'
        ) \
        .alias(f'{parent}_{child}_df')

Now using this function, we can get the ``gene_transcript_df`` DataFrame, shown in :numref:`fig_gene_transcript_df` in a single command:

.. code-block::

    gene_transcript_df = parent_child_summary('gene', 'transcript')

.. figure:: gene_transcript_df.png
   :align: center
   :width: 800
   :name: fig_gene_transcript_df

   A small section of ``gene_transcript_df``

The same function can now be used to generate any parent-child summary DataFrame. For example, we can get the information of all exons of each transcript on chromosome 22 with a single command as shown below. The resulting `transcript_exon_df`` can be seen in :numref:`fig_transcript_exon_df`.

.. code-block::

    transcript_exon_df = parent_child_summary('transcript', 'exon')

.. figure:: transcript_exon_df.png
   :align: center
   :width: 800
   :name: fig_transcript_exon_df

   A small section of ``transcript_exon_df``

Example Continued: Integration with Variant Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Glow has :ref:`data sources to ingest variant data <variant_data>` from common flat file formats such as VCF, BGEN, and PLINK. Combining the power of Glow's variant data sources with the new ``gff`` data source, the users can now seemlessly annotate their variant DataFrames by joining them with an annotation DataFrame in any desired fashion.

As an example, let us load the chromosome 22 variants from the 1000 Genome Project from a VCF file (this VCF which can be obtained :ref:`here <ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/>`). :numref:`fig_variants_df` shows the resulting `variants_df`.

.. code-block::

    vcf_path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"

    variants_df = spark.read \
      .format("vcf") \
      .load(vcf_path) \
      .alias('variants_df')

.. figure:: variants_df.png
   :align: center
   :width: 800
   :name: fig_variants_df

   A small section of ``variants_df``

Now using the following double join query, we can create a DataFrame that, for each variant on a gene on chromosome 22, provides the information of the variant, exon, transcript, and gene on which the variant resides (:numref:`fig_variant_exon_transcript_gene_df`). Note that the first two exploded DataFrames can also be constructed directly from ``parent_child_df``. Here, since we had already defined ``gene_transcrip_df`` and ``transcript_exon_df``, we generated the exploded DataFrames simply by applying an ``explode`` followed by Glow's ``expand_struct`` functions on them.

.. code-block::

    from glow.functions import *

    gene_transcript_exploded_df = gene_transcript_df \
      .withColumn('transcripts', explode('transcripts')) \
      .withColumn('transcripts', expand_struct('transcripts')) \
      .alias('gene_transcript_exploded_df')

    transcript_exon_exploded_df = transcript_exon_df \
      .withColumn('exons', explode('exons')) \
      .withColumn('exons', expand_struct('exons')) \
      .alias('transcript_exon_exploded_df')

    variant_exon_transcript_gene_df = variants_df \
    .join(transcript_exon_exploded_df, (variants_df.start < transcript_exon_exploded_df.exon_end) & (transcript_exon_exploded_df.exon_start < variants_df.end)) \
    .join(gene_transcript_exploded_df, transcript_exon_exploded_df.transcript_id == gene_transcript_exploded_df.transcript_id) \
    .select(
      col('variants_df.contigName').alias('variant_contig'),
      col('variants_df.start').alias('variant_start'),
      col('variants_df.end').alias('variant_end'),
      col('variants_df.referenceAllele'),
      col('variants_df.alternateAlleles'),
      'transcript_exon_exploded_df.exon_id',
      'transcript_exon_exploded_df.exon_start',
      'transcript_exon_exploded_df.exon_end',
      'transcript_exon_exploded_df.transcript_id',
      'transcript_exon_exploded_df.transcript_name',
      'transcript_exon_exploded_df.transcript_start',
      'transcript_exon_exploded_df.transcript_end',
      'gene_transcript_exploded_df.gene_id',
      'gene_transcript_exploded_df.gene_name',
      'gene_transcript_exploded_df.gene_start',
      'gene_transcript_exploded_df.gene_end'
    ) \
    .orderBy(
      'variant_contig',
      'variant_start',
      'variant_end'
    )

.. figure:: variant_exon_transcript_gene_df.png
   :align: center
   :width: 800
   :name: fig_variant_exon_transcript_gene_df

   A small section of ``variants_df``

Other Features and Improvements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In addition to the new ``gff`` reader, Glow 0.4 introduced other features and improvements. The new ``mean_substitute`` Spark SQL function to substitute the missing values of a numeric Spark array with the mean of the non-missing values was introduced. Support for bgzipped fasta files was added to the ``normalize_variants`` transformer. The VCF reader is now able to handle reading file globs that include tabix index files. The ``splitToBiallelic`` option was removed from the VCF reader because  the ``split_multiallelics`` transformer introduced in Glow 0.3 can be used instead. The ``pipe`` transformer was improved not to pipe empty partitions. As a result, users do not need to ``repartition`` or ``coalesce`` when piping VCF files. See `Glow 0.4 Release Notes <https://github.com/projectglow/glow/releases>`_ for a complete list of new features and improvements in Glow 0.4.

Try It!
~~~~~~~
Try Glow 0.4 and its new features `here <https://projectglow.io/>`_.