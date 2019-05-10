load("//bazel/rules:scala.bzl", "scala_library", "scala_binary", "scala_test", "scala_console")
load("//bazel/rules:scala.bzl", "scala_worker", "scala_style_test")
load("//bazel/rules:databricks_test.bzl", "databricks_test", "cross_databricks_test")
load("//bazel:pex/pex.bzl", "pex_binary", "pex_test")
load("//bazel/rules:cross_scala_lib.bzl", "cross_scala_lib", "DEFAULT_CROSS_TREE")

java_library(
    name="sql_resources",
    resources=glob(["src/main/resources/**"]),
)

cross_scala_lib(
    base_name="sql",
    cross_scala_versions=["2.11"],
    cross_trees=["//spark/maven-trees/spark_2.4"],
    srcs=glob(["src/main/scala/**/*.scala"]),
    cross_deps=[
        "//common:common",
        "//extern:extern",
        "//hls/common:common",
        "//spark/sql-extension:sql-extension",
    ],
    deps=[
        "sql_resources",
        "{parent}/org.apache.parquet/parquet-hadoop",
        "{parent}/org.jdbi/jdbi",
        "{parent}/hls/com.databricks.hls/adam-core-spark2_{scala}",
        "{parent}/hls/com.databricks.hls/hadoop-bam",
        "{parent}/hls/org.bdgenomics.utils/utils-intervalrdd-spark2_{scala}",
        "{parent}/hls/org.broadinstitute/gatk",
    ],
    provided_deps=[
        "{parent}/org.apache.spark/spark-catalyst_{scala}",
        "{parent}/org.apache.spark/spark-core_{scala}",
        "{parent}/org.apache.spark/spark-mllib_{scala}",
        "{parent}/org.apache.spark/spark-sql_{scala}",
    ],
    visibility=["//hls:__subpackages__"]
)

cross_scala_lib(
    base_name="sql_test",
    cross_scala_versions=["2.11"],
    cross_trees=["//spark/maven-trees/spark_2.4"],
    srcs=glob(["src/test/scala/**/*.scala"]),
    cross_deps=[
        ":sql",
        "//hls/common:common",
        "//hls/common:common_test",
        "//spark/sql-extension:sql-extension",
        "//testing:testing",
    ],
    deps=[
        "{parent}/org.apache.spark/spark-core_{scala}",
        "{parent}/org.apache.spark/spark-mllib_{scala}",
        "{parent}/org.apache.spark/spark-sql_{scala}",
        "{parent}/hls/org.apache.spark/spark-catalyst_{scala}-tests",
        "{parent}/hls/org.apache.spark/spark-core_{scala}-tests",
        "{parent}/hls/org.apache.spark/spark-sql_{scala}-tests",
        "{parent}/hls/org.bdgenomics.utils/utils-intervalrdd-spark2_{scala}",
        "{parent}/hls/com.databricks.hls/adam-core-spark2_{scala}",
        "{parent}/hls/com.databricks.hls/hadoop-bam",
    ],
    visibility=["//hls:__subpackages__"]
)

SUITES=[
    "com.databricks.bgen.BgenReaderSuite",
    "com.databricks.hls.sql.DefaultSqlExtensionProviderSuite",
    "com.databricks.hls.sql.expressions.FunctionsSuite",
    "com.databricks.hls.sql.expressions.PredicatesSuite",
    "com.databricks.hls.sql.types.RegionSuite",
    "com.databricks.hls.tertiary.MomentAggStateSuite",
    "com.databricks.hls.tertiary.SampleQcExprsSuite",
    "com.databricks.hls.tertiary.VariantQcExprsSuite",
    "com.databricks.hls.tertiary.VariantUtilExprsSuite",
    "com.databricks.vcf.ADAMVCFConverterSuite",
    "com.databricks.vcf.MultiAllelicGenotypeSuite",
    "com.databricks.vcf.MultiAllelicVariantAnnotationSuite",
    "com.databricks.vcf.MultiAllelicVariantCallingAnnotationsSuite",
    "com.databricks.vcf.MultiAllelicVariantSuite",
    "com.databricks.vcf.MultiAllelicVariantContextSuite",
    "com.databricks.vcf.VariantContextToVCFRowConverterSuite",
    "com.databricks.vcf.VCFDatasourceSuite",
    "com.databricks.vcf.VCFFileWriterSuite",
    "com.databricks.vcf.VCFRowToVariantContextConverterSuite",
    "com.databricks.vcf.VCFSchemaInfererSuite",
]

cross_databricks_test(
    scala_versions=["2.11"],
    cross_deps=["sql_test-spark_2.4"],
    data=["//hls/common/test-data:common_test_data"],
    sys_props=[
        "log4j.configuration=file:$(pwd)/extern/src/test/resources/log4j.properties",
    ],
    suites=SUITES,
)
