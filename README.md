<p align="center">
  <img src="static/glow_logo_horiz_color.png" width="140"/>
</p>

<p align="center">
	An open-source toolkit for large-scale genomic analyes
  <br/>
  <a href="https://glow.readthedocs.io/en/latest/?badge=latest"><strong>Explore Koalas docs »</strong></a>
  <br/>
  <br/>
  <a href="https://github.com/projectglow/glow/issues">Issues</a>
  ·
  <a href="https://groups.google.com/forum/#!forum/proj-glow">Mailing list</a>
	.
	<a href="https://join.slack.com/t/proj-glow/shared_invite/enQtNzkwNDE4MzMwMTk5LTE2M2JiMjQ1ZDgyYWNkZTFiY2QyYWE0NGI2YWY3ODY3NmEwNmU5OGQzODcxMDBlYzY2YmYzOGM1YTcyYTRhYjA">Slack</a>
</p>

Glow is an open-source toolkit for working with genomic data at biobank-scale and beyond. The
toolkit is natively built on Apache Spark and Apache Spark SQL.

# Built to scale
Glow makes genomic data work with Spark SQL, the leading engine for working with large structured
datasets. It fits natively into the ecosystem of tools that have enabled thousands of organizations
to scale their workflows to petabytes of data.

# Flexible
Glow works with datasets in common file formats like VCF or BGEN as well as common big data
standards. You can write queries using the native Spark SQL APIs in Python, SQL, R, Java, and Scala.
The same APIs allow you to bring your genomic data together with other datasets like electronic
health records, real world evidence, and medical images. Glow makes it easy to parallelize existing
tools and libraries implemented as command line tools or Pandas functions.

# Easy to get started
If you’ve used Spark before, you don’t need to learn any new APIs to get started with Glow. The
toolkit includes the building blocks that you need to perform the most common analyses right away:

- Datasources for loading VCF and BGEN files into Spark DataFrames
- Functions for performing quality control and data manipulation
- Variant normalization and lift over
- Regression functions
- Integration with Spark ML libraries for population stratification
- Utilities for piping DataFrames through command line tools

[![CircleCI](https://circleci.com/gh/projectglow/glow.svg?style=svg&circle-token=7511f70b2c810a18e88b5c537b0410e82db8617d)](https://circleci.com/gh/projectglow/glow)
[![Documentation
Status](https://readthedocs.org/projects/glow/badge/?version=latest)](https://glow.readthedocs.io/en/latest/?badge=latest)
[![PyPi](https://img.shields.io/pypi/v/glow.py.svg)](https://pypi.org/project/glow.py/)
[![Maven Central](https://img.shields.io/maven-central/v/io.projectglow/glow_2.11.svg)](https://mvnrepository.com/artifact/io.projectglow)
[![Coverage Status](https://codecov.io/gh/projectglow/glow/branch/master/graph/badge.svg)](https://codecov.io/gh/projectglow/glow)

# Building and Testing
This project is built using sbt: https://www.scala-sbt.org/1.0/docs/Setup.html

Start an sbt shell using the `sbt` command.

To compile the main code:
```
compile
```

To run all tests:
```
test
```

To test a specific suite:
```
testOnly *VCFDataSourceSuite
```

If you use IntelliJ, you'll want to set up [scalafmt on save](https://scalameta.org/scalafmt/docs/installation.html).

To test or testOnly in remote debug mode with IntelliJ IDEA set the remote debug configuration in IntelliJ to 'Attach to remote JVM' mode and a specific port number (here the default port number 5005 is used) and then modify the definition of options in groupByHash function in build.sbt to
```
val options = ForkOptions().withRunJVMOptions(Vector("-Xmx1024m")).withRunJVMOptions(Vector("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))
```

To run Python tests, you must install and activate the conda environment in
`python/environment.yml`. You can then run tests from sbt:
```
python/test
```

These tests will run with the same Spark classpath as the Scala tests.
