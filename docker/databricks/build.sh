#!/bin/bash -xue
# Builds all the Docker images
#
# Usage: ./build.sh

DOCKER_HUB="projectglow"
DATABRICKS_RUNTIME_VERSION="10.4"
GLOW_VERSION="1.2.1"
HAIL_VERSION="0.2.93"

# Add commands to build images below
pushd dbr/dbr$DATABRICKS_RUNTIME_VERSION/
docker build -t "${DOCKER_HUB}/minimal:${DATABRICKS_RUNTIME_VERSION}" minimal/
docker build -t "${DOCKER_HUB}/python:${DATABRICKS_RUNTIME_VERSION}" python/
docker build -t "${DOCKER_HUB}/dbfsfuse:${DATABRICKS_RUNTIME_VERSION}" dbfsfuse/
docker build -t "${DOCKER_HUB}/standard:${DATABRICKS_RUNTIME_VERSION}" standard/
docker build -t "${DOCKER_HUB}/with-r:${DATABRICKS_RUNTIME_VERSION}" r/
docker build -t "${DOCKER_HUB}/genomics:${DATABRICKS_RUNTIME_VERSION}" genomics/
docker build -t "${DOCKER_HUB}/databricks-hail:${HAIL_VERSION}" genomics-with-hail/
docker build -t "${DOCKER_HUB}/databricks-glow-minus-ganglia:${GLOW_VERSION}" genomics-with-glow/
docker build -t "${DOCKER_HUB}/databricks-glow:${GLOW_VERSION}" ganglia/
docker build -t "${DOCKER_HUB}/databricks-glow-minus-ganglia:${DATABRICKS_RUNTIME_VERSION}" genomics-with-glow/
docker build -t "${DOCKER_HUB}/databricks-glow:${DATABRICKS_RUNTIME_VERSION}" ganglia/
popd

docker push "${DOCKER_HUB}/databricks-hail:${HAIL_VERSION}"
docker push "${DOCKER_HUB}/databricks-glow:${GLOW_VERSION}"
docker push "${DOCKER_HUB}/databricks-glow:${DATABRICKS_RUNTIME_VERSION}"
