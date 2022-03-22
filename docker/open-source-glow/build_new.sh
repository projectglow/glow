#!/bin/bash -xue
# Builds all the Docker images
#
# Usage: ./build.sh

DOCKER_REPOSITORY="projectglow"

# Add commands to build DBR 9.1 images below
docker build -t "${DOCKER_REPOSITORY}/open-source-base:1.1.2" datamechanics/
docker build -t "${DOCKER_REPOSITORY}/open-source-genomics:1.1.2" genomics/
docker build -t "${DOCKER_REPOSITORY}/open-source-glow:1.1.2" genomics-with-glow/
docker push "${DOCKER_REPOSITORY}/open-source-glow:1.1.2"
