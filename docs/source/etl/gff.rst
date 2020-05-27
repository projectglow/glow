.. _gff:

===================================================
Read Genome Annotations (GFF3) as a Spark DataFrame
===================================================

.. invisible-code-block: python

    from pyspark.sql import Row
    from pyspark.sql.types import *

    import glow
    glow.register(spark)

`GFF3 (Generic Feature Format Version 3) <https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md>`_ (Generic Feature Format Version 3) is a 9-column tab-separated text file format commonly used to store genomic annotations.
The majority of the annotation information typically appears in the ninth column of this format, named the ``attributes`` column, as a semi-colon-separated list of ``<tag>=<value>`` entries. If Spark's standard ``csv`` data source is used to read GFF3 files, the whole list of attribute tag-value pairs will be read as a single string-typed column, making queries on attribute tags cumbersome.

To address this issue, Glow provides the ``gff`` data source. In addition to loading the first 8 columns of GFF3 as properly typed columns, the ``gff`` data source is able to parse all attribute tag-value pairs in the ninth column of GFF3 and create an appropriately typed column for each tag. In each row, this column will contain the value corresponding to that tag (or ``null`` if the tag does not appear in that row).

Like any Spark data source, reading GFF3 files using the ``gff`` data source can be done in a single line of code:

.. invisible-code-block: python

   path = "test-data/gff/test_gff_with_fasta.gff"

.. code-block:: python

   df = spark.read.format("gff").load(path)

.. invisible-code-block: python

   assert_rows_equal(
     df.head(),
     Row(**{'seqId':'NC_000001.11', 'source':'RefSeq', 'type':'region', 'start':0, 'end':248956422, 'score':None, 'strand':'+', 'phase':1, 'ID':'NC_000001.11:1..248956422', 'Name':'1', 'Parent':None, 'Dbxref':['taxon:9606','test'], 'Is_circular':False, 'chromosome':'1', 'description':None, 'gbkey':'Src', 'gene':None, 'gene_biotype':None, 'gene_synonym':None, 'genome':'chromosome', 'mol_type':'genomic DNA', 'product':None, 'pseudo':None, 'test space':None, 'transcript_id':None})
   )

The ``gff`` data source supports all compression formats supported by Spark's ``csv`` data source, including ``.gz`` and ``.bgz`` files.

.. note::
  The ``gff`` data source ignores any comment and directive lines (lines starting with ``#``) in the GFF3 file as well as any FASTA lines that may appear at the end of the file.

Schema
======

1. Inferred schema
~~~~~~~~~~~~~~~~~~

If no user-specified schema is provided (as in the example above), the data source infers the schema as follows:

- The first 8 fields of the schema (base fields) correspond to the first 8 columns of the GFF3 file. Their names, types and order will be as shown below:

  .. _base_fields:
  .. code-block::

     |-- seqId: string (nullable = true)
     |-- source: string (nullable = true)
     |-- type: string (nullable = true)
     |-- start: long (nullable = true)
     |-- end: long (nullable = true)
     |-- score: double (nullable = true)
     |-- strand: string (nullable = true)
     |-- phase: integer (nullable = true)

  .. note:: While the ``start`` column in the GFF3 file is 1-based, the ``start`` field in the DataFrame will be 0-based to match the general practice in Glow.

- The next fields in the inferred schema will be created as the result of parsing the ``attributes`` column of the GFF3 file. Each tag will have its own field in the schema. Fields corresponding to any "official" tag (those referred to as `tags with pre-defined meaning <https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md>`_) come first, followed by fields corresponding to any other tag ("unofficial" tags).

  The complete list of official fields, their data types, and order are as shown below:

  .. code-block::

     |-- ID: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Alias: string (nullable = true)
     |-- Parent: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- Target: string (nullable = true)
     |-- Gap: string (nullable = true)
     |-- DerivesFrom: string (nullable = true)
     |-- Note: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- Dbxref: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- OntologyTerm: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- Is_circular: boolean (nullable = true)


  The unofficial fields will be of ``string`` type.

  .. note::

    - If any of official tags does not appear in any row of the GFF3 file, the corresponding field will be excluded from the inferred schema.
    - The official/unofficial field name will be exactly the same as the tag in terms of letter case.
    - The parser is insensitive to the letter case of the tag, e.g., if the ``attributes`` column in the GFF3 file contains both  ``note`` and ``Note`` tags, they will be both mapped to the same column in the DataFrame. The name of the column will be a random choice of one of the two.

2. User-specified schema
~~~~~~~~~~~~~~~~~~~~~~~~

As with any Spark data source, the user can specify the schema while using the ``gff`` data source using the ``.schema`` command. The user-specified schema can have any subset of the base, official, and unofficial fields. The data source is able to read only the specified base fields and parse out only the specified official and unofficial fields from the ``attributes`` column of the GFF3 file. Here is an example:

.. code-block:: python

   mySchema = StructType(
     [StructField('seqId', StringType()),              # Base field
      StructField('start', LongType()),                # Base field
      StructField('end', LongType()),                  # Base field
      StructField('ID', StringType()),                 # Official field
      StructField('Dbxref', ArrayType(StringType())),  # Official field
      StructField('mol_type', StringType())]           # Unofficial field
   )

   df_user_specified = spark.read.format("gff").schema(mySchema).load(path)

.. invisible-code-block: python

   assert_rows_equal(
     df_user_specified.head(),
     Row(**{'seqId':'NC_000001.11', 'start':0, 'end':248956422, 'ID':'NC_000001.11:1..248956422', 'Dbxref':['taxon:9606','test'], 'mol_type':'genomic DNA'})
   )

.. note::

  - The base field names in the user-specified schema must match the names in this :ref:`list <base_fields>` in a case-sensitive manner.
  - The official and unofficial field will be matched with the corresponding tags in the GFF3 files in a case-and-underscore-insensitive manner. For example, if the GFF3 file contains the official tag name ``db_xref``, a schema field with the name ``dbxref``, ``Db_Xref``, or any other case-and-underscore-insensitive match will correspond to that tag.


.. notebook:: .. etl/gff-reader.html
