.. _workflow-orchestration:

=================================================================================
Spark as a Workflow Orchestrator to Parallelize Command-Line Bioinformatics Tools
=================================================================================

You can use Spark as a workflow orchestrator to manage running a bioinformatics tool across a set of samples or regions of the genome.
Orchestration in this context means each row of the Spark Dataframe defines a task, which could contain individual samples or regions of the genome, as well as parameters for the bioinformatics tool you will run on those regions / samples.
These rows will then be sent out to available cores of the cluster and the tasks will be run in parallel from the command line.

This is similar to submitting jobs to an high performance compute (HPC) cluster. Or applying multithreading across a single node.

.. tip::
   Spark offers these advantages over multithreading / multiprocessing,
   
   - Scale
      - Instead of using one large node with multithreading, you can use many nodes, and you can choose virtual machine type with the best price/performance
   - Efficiency
      - Most bioinformatics tools are single threaded, using only one core of a node
      - Tools that use multithreading often do not fully utilize CPU resources
   - Observability
      - multithreading or multiprocessing is difficult to maintain and debug compared to Spark, where errors are captured in the Spark worker logs  
   
.. important:: 
   - data must be accessible locally on each Spark worker using one of these approaches,
      - downloaded to each node of the cluster
      - cloud storage is mounted on the local filesystem using an open source tool such as `goofys <https://github.com/kahing/goofys>`_ 
      - Databricks' `local file APIs <https://docs.databricks.com/data/databricks-file-system.html#local-file-apis>`_ automatically mounts cloud object storage to the local filesystem
   - bioinformatics tools must be installed on each node of the cluster
      - The best practice is to manage the environment using `Glow Docker Containers <https://github.com/projectglow/glow/tree/master/docker>`_

When to use workflow orchestrator vs pipe transformer architecture
==================================================================

The pipe transformer is designed for embarrassingly parallel processing of individual large datasets, where each row of the dataset is processed in an identical way.
The pipe transformer supports ``VCF`` and ``txt``, and ``csv`` formats, but does not support bioinformatics tools that depend on ``Plink``, ``BGEN`` or other specialized file formats.
Furthermore, the pipe transformer is not designed for parallel processing of distinct samples or regions of the genome. Nor does it offer the flexibility to apply different parameters to those samples or regions.

In these circumstances we recommend using Spark as a workflow orchestrator. This approach is efficient and flexible at parallelizing. It can be a drop-in replacement for HPC or multithreaded workloads.

Examples
========

The following notebooks showcase how to use Spark to parallelize across samples and regions, using bcftools (vcf filtering) and ldpred2 (polygenic risk scoring) as examples

.. notebook:: .. tertiary/parallel_bcftools_filter.html
  :title: Use Spark as a workflow orchestrator to parallelize across samples

