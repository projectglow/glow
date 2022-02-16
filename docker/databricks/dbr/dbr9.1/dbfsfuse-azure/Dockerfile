FROM projectglow/python:9.1

RUN apt-get update \
  && apt-get install -y fuse \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Make sure the USER env variable is set. The files exposed
# by dbfs-fuse will be owned by this user.
# Within the container, the USER is always root.
ENV USER root

# For Azure Databricks, we need to install the init script dcs-wsfs.init
RUN mkdir -p /databricks/scripts/
COPY dcs-wsfs.init /databricks/scripts/
