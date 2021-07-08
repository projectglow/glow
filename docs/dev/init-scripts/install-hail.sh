
#!/bin/bash
set -ex

# Pick up user-provided environment variables, specifically HAIL_VERSION
source /databricks/spark/conf/spark-env.sh

/databricks/python/bin/pip install -U hail==$HAIL_VERSION
hail_jar_path=$(find /databricks/python3 -name 'hail-all-spark.jar')
cp $hail_jar_path /databricks/jars

# Note: This configuration takes precedence since configurations are
# applied in reverse-lexicographic order.
cat <<HERE >/databricks/driver/conf/00-hail.conf
[driver] {
  "spark.kryo.registrator" = "is.hail.kryo.HailKryoRegistrator"
  "spark.hadoop.fs.s3a.connection.maximum" = 5000
  "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
}
HERE

echo $?
