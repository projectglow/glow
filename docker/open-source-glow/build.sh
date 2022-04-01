#!/bin/bash -xue
# Builds all the Docker images
#
# Usage: ./build.sh

DOCKER_HUB="projectglow"
GLOW_VERSION="1.1.2"

# Add commands to build DBR 9.1 images below
docker build -t "${DOCKER_HUB}/open-source-base:${GLOW_VERSION}" datamechanics/
docker build -t "${DOCKER_HUB}/open-source-genomics:${GLOW_VERSION}" genomics/
docker build -t "${DOCKER_HUB}/open-source-glow:${GLOW_VERSION}" genomics-with-glow/
docker push "${DOCKER_HUB}/open-source-glow:${GLOW_VERSION}"
