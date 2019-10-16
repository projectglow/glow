====================
SAIGE and SAIGE-GENE
====================

`SAIGE`_ is a command-line tool written in R for performing genetic association tests on biobank-scale data.
Glow makes it possible to run `single-variant association test method SAIGE`_ and the `region-based association test method SAIGE-GENE`_ using its :ref:`Pipe Transformer <pipe-transformer>`.


.. _`SAIGE`: https://github.com/weizhouUMICH/SAIGE
.. _`single-variant association test method SAIGE`: https://www.nature.com/articles/s41588-018-0184-y
.. _`region-based association test method SAIGE-GENE`: https://www.biorxiv.org/content/10.1101/583278v1


.. _saige-init-script:

Create a SAIGE cluster
======================

Install `HTSLib`_ as well as the SAIGE package, its dependencies, and associated input files on every node in the
cluster using an :ref:`init script <cluster-scoped-init-script>`.
We recommend creating a mount point to store and download these files from cloud storage.
Replace the values in the script with your mount point and local directory.

.. code-block:: sh

    #!/usr/bin/env bash

    set -ex
    set -o pipefail

    # Install SKAT for SAIGE-GENE
    sudo apt-get -y install libboost-all-dev autoconf
    Rscript -e 'install.packages("SKAT", repos="https://cran.us.r-project.org")'

    # Install SAIGE
    cd /opt
    git clone https://github.com/weizhouUMICH/SAIGE
    cd SAIGE
    Rscript extdata/install_packages.R
    R CMD INSTALL *.tar.gz

    # Install HTSLIB
    cd /opt
    git clone https://github.com/samtools/htslib
    cd htslib
    autoheader
    autoconf
    ./configure
    make
    make install

    # Sync SAIGE input files
    aws=/databricks/python2/bin/aws
    source /databricks/init/setup_mount_point_creds.sh <mount-point>
    $aws s3 sync "$MOUNT_ROOT/<saige-input-dir>" "<local-dir>"

.. _`HTSlib`: https://github.com/samtools/htslib
.. _`SAIGE package`: https://github.com/samtools/htslib
.. _`HTSlib`: https://github.com/samtools/htslib

Run association tests
=====================

To parallelize the association test, use Glow's :ref:`Pipe Transformer <pipe-transformer>`.

- Command: A JSON array containing a :ref:`SAIGE <saige-cmd>` or :ref:`SAIGE-GENE <saige-gene-cmd>` script
- Input formatter: VCF
- Output formatter: CSV

  - Header flag: true
  - Delimiter: space

.. code-block:: py

    import glow
    import json

    cmd = json.dumps(["bash", "-c", saige_script])
    output_df = glow.transform(
      "pipe", input_df,
      cmd=cmd,
      input_formatter='vcf',
      in_vcf_header=input_vcf,
      output_formatter='csv',
      out_header='true',
      out_delimiter=' '
    )

You can read the input DataFrame directly from a :ref:`VCF <vcf>` or from a Delta table :ref:`with VCF rows <vcf2delta>`.

Single-variant association test (SAIGE)
---------------------------------------

The following cell shows an example SAIGE script to be piped.
The options are explained in the `single-variant association test docs`_.

.. _saige-cmd:

.. code-block:: sh

    #!/bin/sh
    set -e

    export tmpdir=$(mktemp -d -t vcf.XXXXXX)
    cat - > ${tmpdir}/input.vcf
    bgzip -c ${tmpdir}/input.vcf > ${tmpdir}/input.vcf.gz
    tabix -p vcf ${tmpdir}/input.vcf.gz

    Rscript /opt/SAIGE/extdata/step2_SPAtests.R \
        --vcfFile=${tmpdir}/input.vcf.gz \
        --vcfFileIndex=${tmpdir}/input.vcf.gz.tbi \
        --SAIGEOutputFile=${tmpdir}/output.txt \
        --sampleFile=<local-dir>/input/sampleIds.txt \
        --GMMATmodelFile=<local-dir>/step-1/output.rda \
        --varianceRatioFile=<local-dir>/step-1/output.varianceRatio.txt \
        --vcfField=GT \
        --chrom=22 \
        --minMAF=0.0001 \
        --minMAC=1 \
        --numLinesOutput=2 \
        >&2

    cat ${tmpdir}/output.txt
    rm -rf ${tmpdir}

.. _`single-variant association test docs`: https://github.com/weizhouUMICH/SAIGE/wiki/Genetic-association-tests-using-SAIGE#step-2-performing-the-single-variant-association-tests

Region-based association test (SAIGE-GENE)
------------------------------------------

Before piping, partition the input DataFrame by region or gene, with each partition containing the sorted, distinct, and
relevant set of VCF rows. The regions and corresponding variants should match those in the group file referenced in the
SAIGE-GENE script. Regions can be derived from any source, such as gene annotations from the
:ref:`SnpEff pipeline <snpeff-pipeline>`.

.. code-block:: py

    input_df = genotype_df.join(region_df, ["contigName", "start", "end"]) \
      .repartition("geneName") \
      .select("contigName", "start", "end", "referenceAllele", "alternateAlleles", "genotypes") \
      .sortWithinPartitions("contigName", "start")

The following cell shows an example SAIGE-GENE script to be piped.
The options are explained in the `region-based association test docs`_.

.. _saige-gene-cmd:

.. code-block:: sh

    #!/bin/sh
    set -e

    export tmpdir=$(mktemp -d -t vcf.XXXXXX)
    uniq -w 100 - > ${tmpdir}/input.vcf
    bgzip -c ${tmpdir}/input.vcf > ${tmpdir}/input.vcf.gz
    tabix -p vcf ${tmpdir}/input.vcf.gz

    Rscript /opt/SAIGE/extdata/step2_SPAtests.R \
        --vcfFile=${tmpdir}/input.vcf.gz \
        --vcfFileIndex=${tmpdir}/input.vcf.gz.tbi \
        --SAIGEOutputFile=${tmpdir}/output.txt \
        --groupFile=${tmpdir}/groupFile.txt \
        --sampleFile=<local-dir>/input/sampleIds.txt \
        --GMMATmodelFile=<local-dir>/step-1/output.rda \
        --varianceRatioFile=<local-dir>/step-1/output.varianceRatio.txt \
        --sparseSigmaFile=<local-dir>/step-1/output.varianceRatio.txt_relatednessCutoff_0.125_2000_randomMarkersUsed.sparseSigma.mtx \
        --vcfField=GT \
        --chrom=22 \
        --minMAF=0 \
        --minMAC=0.5 \
        --maxMAFforGroupTest=0.5 \
        --numLinesOutput=2 \
        >&2

    cat ${tmpdir}/output.txt
    rm -rf ${tmpdir}

After piping, disambiguate the association results for each gene in the output DataFrame.

.. code-block:: python

    from pyspark.sql import Window

    window_spec = Window.partitionBy("Gene")
    assoc_df = output_df.withColumn("numMarkerIDs", size(split("markerIDs", ";"))) \
      .withColumn("maxNumMarkerIDs", max("numMarkerIDs").over(window_spec)) \
      .filter("numMarkerIDs = maxNumMarkerIDs") \
      .drop("numMarkerIDs", "maxNumMarkerIDs") \
      .drop_duplicates(["Gene"])

.. _`region-based association test docs`: https://github.com/weizhouUMICH/SAIGE/wiki/Genetic-association-tests-using-SAIGE#step-2-performing-the-region--or-gene-based-association-tests

Example notebooks
=================

.. notebook:: genomics/null-fit.html
  :title: Null logistic mixed model fit notebook

.. notebook:: ../_static/notebooks/tertiary/saige.html
  :title: SAIGE notebook

.. notebook:: ../_static/notebooks/tertiary/saige-gene.html
  :title: SAIGE-GENE notebook

.. include:: /shared/products.rst
