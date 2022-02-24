# ===== For the runtime environment for this image we need the databricks azure setup ==============

FROM projectglow/genomics:9.1 AS builder

# ===== Set up Hail ================================================================================

# earliest Hail version supported by Spark3 is 0.2.67
ENV HAIL_VERSION=0.2.78
 
RUN apt-get update && apt-get install -y \
     openjdk-8-jre-headless \
     g++ \
     libopenblas-base liblapack3 \
     liblz4-1 liblz4-dev liblz4-tool \
     rsync python-setuptools

RUN /databricks/python3/bin/pip install hail
RUN HAIL_JAR_PATH=$(find /databricks/python3 -name 'hail-all-spark.jar') && \
  mkdir -p /databricks/jars && \
  cp $HAIL_JAR_PATH /databricks/jars/

RUN mkdir -p /databricks/driver/conf/
COPY 00-hail.conf /databricks/driver/conf/

# ===== Set up Selenium for Bokeh (Bokeh itself is included in Hail) ===============================

ENV SELENIUM_VERSION=3.141.0

RUN /databricks/python3/bin/pip install selenium==$SELENIUM_VERSION
