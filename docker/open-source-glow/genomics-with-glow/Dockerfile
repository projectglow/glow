FROM projectglow/open-source-genomics:1.2.1 AS genomics
LABEL author="Edoardo Giacopuzzi"
LABEL contact="edoardo.giacopuzzi@fht.org"
LABEL spark_version="3.2.1"
LABEL hadoop_version="3.3.1"
LABEL java_version="11"
LABEL scala_version="2.12"
LABEL deltalake_version="1.0.0"
LABEL glowgr_version="spark3-1.2.1"
LABEL description="Spark with Glow support and glow.py"

USER root
WORKDIR /opt

ENV GLOW_VERSION=1.2.1
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
