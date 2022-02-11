#!/bin/bash -xue
# Builds all the Docker images
#
# Usage: ./build.sh

DOCKER_REPOSITORY="projectglow"

# Add commands to build DBR 9.1 images below
pushd dbr/dbr9.1/
docker build -t "${DOCKER_REPOSITORY}/minimal:9.1" minimal/
docker build -t "${DOCKER_REPOSITORY}/python:9.1" python/
docker build -t "${DOCKER_REPOSITORY}/dbfsfuse:9.1" dbfsfuse/
docker build -t "${DOCKER_REPOSITORY}/standard:9.1" standard/
docker build -t "${DOCKER_REPOSITORY}/with-r:9.1" r/
docker build -t "${DOCKER_REPOSITORY}/genomics:9.1" genomics/
docker build -t "${DOCKER_REPOSITORY}/databricks-hail:0.2.78" genomics-with-hail/
docker build -t "${DOCKER_REPOSITORY}/databricks-glow:1.1.2" genomics-with-glow/
docker build -t "${DOCKER_REPOSITORY}/databricks-glow:9.1" genomics-with-glow/
popd

docker push "${DOCKER_REPOSITORY}/databricks-hail:0.2.78"
docker push "${DOCKER_REPOSITORY}/databricks-glow:1.1.2"
docker push "${DOCKER_REPOSITORY}/databricks-glow:9.1"


