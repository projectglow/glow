# ===== For the runtime environment for this image we need the databricks azure setup ============== 

FROM projectglow/genomics:9.1 AS builder

ENV GLOW_VERSION=1.1.2

# ===== Install python dependencies for Glow =======================================================
# once available, we want specify that the earliest version is 1.1.0

RUN /databricks/python3/bin/pip install glow.py==$GLOW_VERSION

ENV BIOINFOKIT_VERSION=0.8.5
RUN /databricks/python3/bin/pip install bioinfokit==$BIOINFOKIT_VERSION

# ===== Set up scala dependencies for Glow =========================================================

ENV HADOOP_BAM_VERSION=7.9.1
ENV HTSJDK_VERSION=2.21.2
ENV PICARD_VERSION=2.23.3
ENV JDBI_VERSION=2.78

RUN mkdir /databricks/jars
RUN cd /databricks/jars && curl -O \
https://repo1.maven.org/maven2/io/projectglow/glow-spark3_2.12/${GLOW_VERSION}/glow-spark3_2.12-${GLOW_VERSION}.jar 
RUN cd /databricks/jars && curl -O \
https://repo1.maven.org/maven2/org/seqdoop/hadoop-bam/${HADOOP_BAM_VERSION}/hadoop-bam-${HADOOP_BAM_VERSION}.jar
RUN cd /databricks/jars && curl -O \ 
https://repo1.maven.org/maven2/com/github/samtools/htsjdk/${HTSJDK_VERSION}/htsjdk-${HTSJDK_VERSION}.jar
RUN cd /databricks/jars && curl -O \ 
https://repo1.maven.org/maven2/com/github/broadinstitute/picard/${PICARD_VERSION}/picard-${PICARD_VERSION}.jar
RUN cd /databricks/jars && curl -O \
https://repo1.maven.org/maven2/org/jdbi/jdbi/${JDBI_VERSION}/jdbi-${JDBI_VERSION}.jar

# ===== Set up needed Spark config for scala jars ==================================================

ENV JAVA_OPTS="-Dspark.executor.extraClassPath=/databricks/jars/glow-spark3_2.12-${GLOW_VERSION}.jar,/databricks/jars/hadoop-bam-${HADOOP_BAM_VERSION}.jar,/databricks/jars/htsjdk-${HTSJDK_VERSION}.jar,/databricks/jars/picard-${PICARD_VERSION}.jar,/databricks/jars/jdbi-${JDBI_VERSION}.jar \
               -Dspark.driver.extraClassPath=/databricks/jars/glow-spark3_2.12-${GLOW_VERSION}.jar,/databricks/jars/hadoop-bam-${HADOOP_BAM_VERSION}.jar,/databricks/jars/htsjdk-${HTSJDK_VERSION}.jar,/databricks/jars/picard-${PICARD_VERSION}.jar,/databricks/jars/jdbi-${JDBI_VERSION}.jar \
               -Dspark.serializer=org.apache.spark.serializer.KryoSerializer \
               -Dspark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec,org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec"

# ===== Set up liftOver (used by standard Glow examples) ===========================================

RUN mkdir /opt/liftover
RUN curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain

# ===== Set up bedtools as desired by many Glow users ==============================================

ENV BEDTOOLS_VERSION=2.30.0
ENV PATH=/databricks/conda/envs/dcs-minimal/bin/:$PATH
RUN cd /opt && git clone --depth 1 --branch v${BEDTOOLS_VERSION} https://github.com/arq5x/bedtools2.git bedtools-${BEDTOOLS_VERSION} 
RUN cd /opt/bedtools-${BEDTOOLS_VERSION} && make 

# Add bedtools path to the enviroment

ENV PATH=/opt/bedtools-${BEDTOOLS_VERSION}:$PATH 

WORKDIR /root/
