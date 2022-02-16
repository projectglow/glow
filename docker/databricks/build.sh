#!/bin/bash -xue
# Builds all the Docker images
#
# Usage: ./build.sh

<<<<<<< HEAD
<<<<<<< HEAD
DOCKER_REPOSITORY="projectglow"
=======
=======
>>>>>>> 41ae0b9 (Fetch upstream)
DATABRICKS_RUNTIME_VERSION="9.1"
GLOW_VERSION="1.1.2"
HAIL_VERSION="0.2.85"

# build 9.1 LTS / Spark 3.1.2 images 

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

# build 10.4 LTS / Spark 3.2.1 images

DOCKER_HUB="projectglow"
DATABRICKS_RUNTIME_VERSION="10.4"
GLOW_VERSION="1.2.1"
HAIL_VERSION="0.2.93"
>>>>>>> c92fa4e (Update build.sh to build 9.1 LTS and 10.4 LTS containers)

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

<<<<<<< HEAD
docker push "${DOCKER_REPOSITORY}/databricks-hail:0.2.78"
docker push "${DOCKER_REPOSITORY}/databricks-glow:1.1.2"
docker push "${DOCKER_REPOSITORY}/databricks-glow:9.1"
=======
docker push "${DOCKER_HUB}/databricks-hail:${HAIL_VERSION}"
docker push "${DOCKER_HUB}/databricks-glow:${GLOW_VERSION}"
docker push "${DOCKER_HUB}/databricks-glow:${DATABRICKS_RUNTIME_VERSION}"


<<<<<<< HEAD
>>>>>>> c92fa4e (Update build.sh to build 9.1 LTS and 10.4 LTS containers)
=======
=======
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
docker build -t "${DOCKER_REPOSITORY}/databricks-glow-ganglia:1.1.2" ganglia/
docker build -t "${DOCKER_REPOSITORY}/databricks-glow:9.1" genomics-with-glow/
docker build -t "${DOCKER_REPOSITORY}/databricks-glow-ganglia:9.1" ganglia/
popd

docker push "${DOCKER_REPOSITORY}/databricks-hail:0.2.78"
docker push "${DOCKER_REPOSITORY}/databricks-glow:1.1.2"
docker push "${DOCKER_REPOSITORY}/databricks-glow:9.1"
>>>>>>> f6791fc (Fetch upstream)
>>>>>>> 41ae0b9 (Fetch upstream)


