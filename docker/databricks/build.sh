#!/bin/bash -xue
# Builds all the Docker images
#
# Usage: ./build.sh

DOCKER_REPOSITORY="projectglow"

# Build the DBR 7.3 images
pushd dbr/dbr7.3/
docker build -t "${DOCKER_REPOSITORY}/minimal:7.3" minimal/
docker build -t "${DOCKER_REPOSITORY}/python:7.3" python/
docker build -t "${DOCKER_REPOSITORY}/dbfsfuse:7.3" dbfsfuse/
docker build -t "${DOCKER_REPOSITORY}/standard:7.3" standard/
docker build -t "${DOCKER_REPOSITORY}/with-r:7.3" r/
docker build -t "${DOCKER_REPOSITORY}/genomics:7.3" genomics/
docker build -t "${DOCKER_REPOSITORY}/genomics-with-glow:7.3" genomics-with-glow/
popd

# Add commands to build DBR 9.0 images below
pushd dbr/dbr9.0/
docker build -t "${DOCKER_REPOSITORY}/minimal:9.0" minimal/
docker build -t "${DOCKER_REPOSITORY}/python:9.0" python/
docker build -t "${DOCKER_REPOSITORY}/dbfsfuse:9.0" dbfsfuse/
docker build -t "${DOCKER_REPOSITORY}/standard:9.0" standard/
docker build -t "${DOCKER_REPOSITORY}/with-r:9.0" r/
docker build -t "${DOCKER_REPOSITORY}/genomics:9.0" genomics/
docker build -t "${DOCKER_REPOSITORY}/genomics-with-hail:9.0" genomics-with-hail/
# UNCOMMENT THE COMMAND BELOW ONCE GLOW 1.1.0 is available
#docker build -t "${DOCKER_REPOSITORY}/genomics-with-glow:9.0" genomics-with-glow/
popd

# TODO: Add two commands to push the DBR 7.3 Glow and DBR 9.0 Glow & Hail images to Dockerhub
docker push "${DOCKER_REPOSITORY}/genomics-wish-glow:7.3"
docker push "${DOCKER_REPOSITORY}/genomics-with-hail:9.0"
#docker push "${DOCKER_REPOSITORY}/genomics-wish-glow:9.0"


