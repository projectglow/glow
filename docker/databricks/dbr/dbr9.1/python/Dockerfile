FROM projectglow/minimal:9.1

# Suppress interactive configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Installs python 3.8 and virtualenv for Spark and Notebooks
RUN apt-get update \
  && apt-get install -y \
    python3.8 \
    virtualenv \
    git-all \
    make \
    automake \
    gcc \
    g++ \
    subversion \
    python3-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Initialize the default environment that Spark and notebooks will use
RUN virtualenv -p python3.8 --system-site-packages /databricks/python3

# These python libraries are used by Databricks notebooks and the Python REPL
# You do not need to install pyspark - it is injected when the cluster is launched
# Versions are intended to reflect DBR 9.1
RUN /databricks/python3/bin/pip install \
  six==1.15.0 \
  # downgrade ipython to maintain backwards compatibility with 7.x and 8.x runtimes
  ipython==7.22.0 \
  numpy==1.19.2 \
  pandas==1.2.4 \
  pyarrow==4.0.0 \
  matplotlib==3.4.2 \
  jinja2==2.11.3 \
  mlflow==1.19.0

ENV MLFLOW_TRACKING_URI=databricks

# Specifies where Spark will look for the python process
ENV PYSPARK_PYTHON=/databricks/python3/bin/python3
