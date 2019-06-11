[![CircleCI](https://circleci.com/gh/databricks/spark-genomics.svg?style=svg&circle-token=31dc0fb939711565583c10d783f424ad2fb81e38)](https://circleci.com/gh/databricks/spark-genomics)

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
