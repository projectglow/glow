#builds off Docker images for Apache Spark by Data Mechanics
#this image includes connectors to 
# - Azure blob and datalake
# - Google cloud storage
# - Amazon AWS S3
# - Snowflake
# - deltalake
#to learn more, see https://hub.docker.com/r/datamechanics/spark
FROM gcr.io/datamechanics/spark:3.2.1-hadoop-3.3.1-java-11-scala-2.12-python-3.8-dm17
LABEL author="Edoardo Giacopuzzi"
LABEL contact="edoardo.giacopuzzi@fht.org"
LABEL spark_version="3.2.1"
LABEL hadoop_version="3.3.1"
LABEL java_version="11"
LABEL scala_version="2.12"
LABEL deltalake_version="1.0.0"
LABEL glowgr_version="spark3-1.2.1"
LABEL description="Spark with Glow support and glow.py"

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

#Install some glow.py dependencies in the base conda env
RUN conda update conda \
&& conda install anaconda \
&& conda update --all \
&& conda config --set channel_priority false \
&& conda install -c bioconda -c conda-forge \
nptyping=1.3.0 \
numpy>=1.18.1 \
opt_einsum>=3.2.0 \
pandas>=1.0.1 \
statsmodels>=0.10.0 \
typeguard=2.9.1 \
pyarrow>=1.0.1 
