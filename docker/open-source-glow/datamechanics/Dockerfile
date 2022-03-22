#builds off Docker images for Apache Spark by Data Mechanics
#this image includes connectors to 
# - Azure blob and datalake
# - Google cloud storage
# - Amazon AWS S3
# - Snowflake
# - deltalake
#to learn more, see https://hub.docker.com/r/datamechanics/spark
FROM gcr.io/datamechanics/spark:3.1.2-hadoop-3.2.0-java-11-scala-2.12-python-3.8-dm16
LABEL author="Edoardo Giacopuzzi"
LABEL contact="edoardo.giacopuzzi@fht.org"
LABEL spark_version="3.1.2"
LABEL hadoop_version="3.2.0"
LABEL java_version="11"
LABEL scala_version="2.12"
LABEL deltalake_version="1.0.0"
LABEL glowgr_version="spark3-1.1.2"
LABEL description="Spark with Glow support and glow.py"

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

#Install some glow.py dependencies in the base conda env
RUN conda remove chardet asn1crypto \
&& conda update conda \
&& conda config --set channel_priority false \
&& conda update --all \
&& conda install -c bioconda -c conda-forge \
nptyping=1.3.0 \
numpy=1.18.1 \
opt_einsum=3.2.0 \
pandas=1.0.1 \
statsmodels=0.10.0 \
typeguard=2.9.1 \
pyarrow=1.0.1 

USER root
WORKDIR /opt

ENV GLOW_VERSION=1.1.2
ENV SCALA_LOGGING_VERSION=3.7.2
ENV PICARD_VERSION=2.23.3
ENV HTSJDK_VERSION=2.21.2
ENV NETTY_VERSION=3.9.9
ENV JDBI_VERSION=2.78
ENV HADOOP_BAM_VERSION=7.9.2

#Download Glow JAR and its dependencies
RUN wget https://repo1.maven.org/maven2/io/projectglow/glow-spark3_2.12/${GLOW_VERSION}/glow-spark3_2.12-${GLOW_VERSION}.jar \
&& wget https://repo1.maven.org/maven2/com/typesafe/scala-logging/scala-logging_2.12/${SCALA_LOGGING_VERSION}/scala-logging_2.12-${SCALA_LOGGING_VERSION}.jar \
&& wget https://repo1.maven.org/maven2/com/github/broadinstitute/picard/${PICARD_VERSION}/picard-${PICARD_VERSION}.jar \
&& wget https://repo1.maven.org/maven2/com/github/samtools/htsjdk/${HTSJDK_VERSION}/htsjdk-${HTSJDK_VERSION}.jar \
&& wget https://repo1.maven.org/maven2/io/netty/netty/${NETTY_VERSION}.Final/netty-${NETTY_VERSION}.Final.jar \
&& wget https://repo1.maven.org/maven2/org/jdbi/jdbi/${JDBI_VERSION}/jdbi-${JDBI_VERSION}.jar \
&& wget https://repo1.maven.org/maven2/org/seqdoop/hadoop-bam/${HADOOP_BAM_VERSION}/hadoop-bam-${HADOOP_BAM_VERSION}.jar \
&& mv *.jar /opt/spark/jars

#Install Glow python interface
RUN pip3 install --upgrade pip
RUN pip3 install glow.py==${GLOW_VERSION}
