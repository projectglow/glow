# ===== For the runtime environment for this image we need the databricks azure setup ==============

FROM projectglow/genomics-databricks-azure:8.x AS builder

# ===== Set up Hail ================================================================================

ENV HAIL_VERSION=0.2.74
# we will want to specify that the earliest Hail version is 0.2.65
ENV SCALA_VERSION=2.12
ENV SPARK_VERSION=3.1.2

RUN apt-get update && apt-get install -y \
    openjdk-8-jre-headless \
    g++ \
    libopenblas-base liblapack3 \
    liblz4-1 liblz4-dev liblz4-tool \
    rsync python-setuptools

RUN cd / && git clone --depth 1 --branch ${HAIL_VERSION} https://github.com/hail-is/hail.git && cd hail
ENV PATH=/databricks/conda/envs/dcs-minimal/bin/:$PATH
RUN cd /hail/hail && make install-on-cluster HAIL_COMPILE_NATIVES=1 SCALA_VERSION=$SCALA_VERSION SPARK_VERSION=$SPARK_VERSION

RUN /databricks/conda/envs/dcs-minimal/bin/pip install hail==$HAIL_VERSION
RUN mkdir /databricks/jars
RUN cp /hail/hail/python/hail/backend/hail-all-spark.jar /databricks/jars

RUN HAIL_HOME=$(/databricks/conda/envs/dcs-minimal/bin/pip show hail | grep Location | awk -F' ' '{print $2 "/hail"}')

ENV JAVA_OPTS="-Dspark.executor.extraClassPath=/databricks/jars/hail-all-spark.jar \
               -Dspark.driver.extraClassPath=/databricks/jars/hail-all-spark.jar \
               -Dspark.kryo.registrator=is.hail.kryo.HailKryoRegistrator \
               -Dspark.hadoop.fs.s3a.connection.maximum=5000 \
               -Dspark.serializer=org.apache.spark.serializer.KryoSerializer"

# Required to by Hail runtime 
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.1

# Required to encapsulate setting the Spark configuration in the container 
SHELL ["/bin/bash", "-c"]
RUN mkdir -p /databricks/driver/conf
RUN echo -e '\
  [driver] {\n\
    "spark.kryo.registrator" = "is.hail.kryo.HailKryoRegistrator"\n\
    "spark.hadoop.fs.s3a.connection.maximum" = 5000\n\
    "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"\n\
  }\n\
  ' > /databricks/driver/conf/00-hail-spark-driver-defaults.conf

# ===== Set up Selenium for Bokeh (Bokeh itself is included in Hail) ===============================

ENV SELENIUM_VERSION=3.141.0

RUN /databricks/conda/envs/dcs-minimal/bin/pip install selenium==$SELENIUM_VERSION

WORKDIR /root/


