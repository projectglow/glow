FROM databricksruntime/genomics-azure:8.x
# ===== Set up Hail ================================================================================

ENV HAIL_VERSION=0.2.74
ENV SCALA_VERSION=2.12
ENV SPARK_VERSION=3.1.2
# maybe we can specify that the earliest version is 0.2.65

RUN apt-get update && apt-get install -y \
    openjdk-8-jre-headless \
    g++ \
    libopenblas-base liblapack3 liblz4-1

RUN git clone https://github.com/hail-is/hail.git
RUN cd hail
RUN git checkout tags/$HAIL_VERSION
RUN cd hail
RUN make install-on-cluster HAIL_COMPILE_NATIVES=1 SCALA_VERSION=$SCALA_VERSION SPARK_VERSION=$SPARK_VERSION
RUN hail_jar_path=$(find ./ -name 'hail-all-spark.jar') 

RUN /databricks/conda/envs/dcs-minimal/bin/pip install hail==$HAIL_VERSION
RUN hail_jar_path=$(find /databricks/conda/envs/dcs-minimal/lib -name 'hail-all-spark.jar')
RUN mkdir /databricks/jars
COPY $hail_jar_path /databricks/jars

RUN HAIL_HOME=$(/databricks/python3/bin/pip show hail | grep Location | awk -F' ' '{print $2 "/hail"}')

# RUN echo -e '\
# [driver] {\n\
#   "spark.kryo.registrator" = "is.hail.kryo.HailKryoRegistrator"\n\
#   "spark.hadoop.fs.s3a.connection.maximum" = 5000\n\
#   "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"\n\
# }\n\
# ' > /databricks/driver/conf/00-hail.conf




