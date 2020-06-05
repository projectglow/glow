=============================================================================
Glow 0.4.0 Enables Integration of Genetic Variant Dand Genome Annotation Data
=============================================================================

| Author: `Kiavash Kianfar <https://github.com/kianfar77>`_
| June 2, 2020

.. _`GFF3`: https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md
Glow 0.4.0 was released on May 20, 2020. This blog is mainly focused on the highlight of this release, which is the introduction of genomic annotation data ingest capability from the `GFF3 (Generic Feature Format Version 3) <https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md>`_ flat file format. This release includes some other feature and usability improvements, which we will mention at the end.

`GFF3`_ is a sequence annotation flat file format proposed by the `Sequence Ontology Project <http://www.sequenceontology.org/>`_ in 2013, which since has become the de facto format for genome annotation and is widely used by genome browsers and databases such as NCBI `RefSeq <https://www.ncbi.nlm.nih.gov/refseq/>`_ and `GenBank <https://www.ncbi.nlm.nih.gov/genbank/>`_. `GFF3`_, a 9-column tab-separated text format, typically carries the majority of annotation data in the ninth column, called ``attributes``, as a semi-colon-separated list of ``<tag>=<value>`` entries. As a result, although `GFF3`_ files can be read as Spark DataFrames using Spark SQL's standard ``csv`` data source, the resulting DataFrame's schema is not very useful for query and data manipulation of annotation data as the whole list of attribute tag-value pairs for each sequence will appear as a single semi-colon-separated string in the `attributes` column of the DataFrame.

Glow 0.4.0 has been equipped with the new and flexible ``gff`` Spark SQL data source to address this challenge and create a smooth `GFF3`_ ingest and query experience. While reading the `GFF3`_  file, the ``gff`` data source parses the ``attributes`` column of the file to create an appropriately typed column for each tag, which, for each row, will contain the value corresponding to that tag in that row (or ``null`` if the tag does not appear in the row). Consequently, all tags in the `GFF3`_ ``attributes`` column will have their own corresponding column in the Spark DataFrame, making annotation data query and manipulation much easier.

.. _gff3_ingest:

Ingest GFF3 Annotation Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Like any other Spark data source, reading `GFF3`_ files using Glow's ``gff`` data source can be done in a single line of code. As an example, the sequence annotation data for the Homo Sapiens genome assembly GRCh38.p13, which we have downloaded in the `GFF3`_ format from `RefSeq ftp site <https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/reference/GCF_000001405.39_GRCh38.p13/>`_, can be ingested as shown below. Here, we have additionally filtered the results to chromosome 22 and given an alias to the resulting DataFrame to use it later in continuation of this example.

.. code-block::

  gff_path = '/databricks-datasets/gffs/GCF_000001405.39_GRCh38.p13_genomic.gff.gz'

  annotations_df = spark.read.format("gff").load(gff_path) \
      .filter("seqid = 'NC_000022.11'") \
      .alias('annotations_df')

.. figure:: annotations_df.png
   :align: center
   :width: 800
   :name: fig_annotations_df

Let us have a closer look at the schema of the resulting Dataframe:

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


Example: Gene Transcripts and Transcript Exons
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    with_parent_df = annotations_df \
    .join(
      annotations_df.select('id', 'type', 'name', 'start', 'end').alias('parent_df'),
      col('annotations_df.parent')[0] == col('parent_df.id') # all annotation_df rows have a single parent
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
    .alias('with_parent_df')

    display(with_parent_df)

.. code-block::

    gene_transcript_df = with_parent_df \
    .select(
      'seqid',
      col('parent_id').alias('gene_id'),
      col('parent_name').alias('gene_name'),
      col('parent_start').alias('gene_start'),
      col('parent_end').alias('gene_end'),
      col('id').alias('transcript_id'),
      col('start').alias('transcript_start'),
      col('end').alias('transcript_end'),
    ) \
    .where("type == 'transcript' and parent_type == 'gene'") \
    .orderBy(
      'seqid',
      'gene_start',
      'gene_end',
      'transcript_start',
      'transcript_end'
    ) \
    .alias('gene_transcript_df')

    display(gene_transcript_df)

.. code-block::

    transcript_exon_df = with_parent_df \
    .select(
      'seqid',
      col('parent_id').alias('transcript_id'),
      col('parent_name').alias('transcript_name'),
      col('parent_start').alias('transcript_start'),
      col('parent_end').alias('transcript_end'),
      col('id').alias('exon_id'),
      col('start').alias('exon_start'),
      col('end').alias('exon_end'),
    ) \
    .where("type == 'exon' and parent_type == 'transcript'") \
    .orderBy(
      'seqid',
      'exon_start',
      'exon_end',
      'transcript_start',
      'transcript_end'
    ) \
    .alias('transcript_exon_df')

    display(transcript_exon_df)

.. code-block::

    gene_df = gene_transcript_df  \
    .groupBy(
      'seqid',
      'gene_id',
      'gene_name',
      'gene_start',
      'gene_end',
    ) \
    .agg(collect_list(struct('transcript_id', 'transcript_start', 'transcript_end')).alias('transcripts')) \
    .orderBy('gene_start', 'gene_end') \
    .alias('gene_df')

    display(gene_df)

.. code-block::

    transcript_df = with_parent_df \
    .where("type == 'exon' and parent_type == 'transcript'") \
    .groupBy(
      'seqid',
      col('parent').alias('transcript_id'),
      col('parent_name').alias('transcript_name'),
      col('parent_start').alias('transcript_start'),
      col('parent_end').alias('transcript_end'),
    ) \
    .agg(collect_list(struct('id', 'start', 'end')).alias('transcript_exons')) \
    .orderBy('transcript_start', 'transcript_end')

    display(transcript_df)

Example Continued: Integration with Variant Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block::

    variant_exons_transcript_gene_df = variants_df \
    .join(transcript_exon_df, (variants_df.start < transcript_exon_df.exon_end) & (transcript_exon_df.exon_start < variants_df.end)) \
    .join(gene_transcript_df, transcript_exon_df.transcript_id == gene_transcript_df.transcript_id) \
    .select(
      col('variants_df.contigName').alias('variant_contig'),
      col('variants_df.start').alias('variant_start'),
      col('variants_df.end').alias('variant_end'),
      col('variants_df.referenceAllele'),
      col('variants_df.alternateAlleles'),
      'transcript_exon_df.exon_id',
      'transcript_exon_df.exon_start',
      'transcript_exon_df.exon_end',
      'transcript_exon_df.transcript_id',
      'transcript_exon_df.transcript_name',
      'transcript_exon_df.transcript_start',
      'transcript_exon_df.transcript_end',
      'gene_transcript_df.gene_id',
      'gene_transcript_df.gene_name',
      'gene_transcript_df.gene_start',
      'gene_transcript_df.gene_end'
    ) \
    .orderBy(
      'variant_contig',
      'variant_start',
      'variant_end'
    )

    display(variant_exons_transcript_gene_df)

Other Improvements
~~~~~~~~~~~~~~~~~~
Glow 0.4.0 also

Try It!
~~~~~~~
Try Glow 0.4.0 and its new features `here <https://projectglow.io/>`_.