# ===== For the runtime environment for this image we need the databricks azure setup ==============

FROM projectglow/genomics:10.4 AS builder

# ===== Set up Hail ================================================================================

# earliest Hail version supported by Spark3 is 0.2.67
# note: this does not yet work with Spark 3.2.1, if you have trouble drop back to 0.2.85
ENV HAIL_VERSION=0.2.93
 
RUN apt-get update && apt-get install -y \
     openjdk-8-jre-headless \
     g++ \
     libopenblas-base liblapack3 \
     liblz4-1 liblz4-dev liblz4-tool \
     rsync python-setuptools

RUN /databricks/python3/bin/pip install \
  hail==$HAIL_VERSION

RUN HAIL_JAR_PATH=$(find /databricks/python3 -name 'hail-all-spark.jar') && \
  mkdir -p /databricks/jars && \
  cp $HAIL_JAR_PATH /databricks/jars/

RUN mkdir -p /databricks/driver/conf/
COPY 00-hail.conf /databricks/driver/conf/

# ===== Set up Selenium for Bokeh (Bokeh itself is included in Hail) ===============================

ENV SELENIUM_VERSION=3.141.0

RUN /databricks/python3/bin/pip install selenium==$SELENIUM_VERSION
