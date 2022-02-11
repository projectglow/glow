<<<<<<< HEAD
<<<<<<< HEAD
FROM amd64/ubuntu:20.04

RUN apt-get update \
  && apt-get install --yes \
    ca-certificates-java=20190405ubuntu1 \
=======
=======
>>>>>>> 41ae0b9 (Fetch upstream)
FROM ubuntu:20.04

RUN apt-get update \
  && apt-get install --yes \
<<<<<<< HEAD
>>>>>>> 6908c68 (Add 9.1 LTS Dockerfiles to Databricks tree)
=======
=======
FROM amd64/ubuntu:20.04

RUN apt-get update \
  && apt-get install --yes \
    ca-certificates-java=20190405ubuntu1 \
>>>>>>> f6791fc (Fetch upstream)
>>>>>>> 41ae0b9 (Fetch upstream)
    openjdk-8-jdk \
    iproute2 \
    bash \
    sudo \
    coreutils \
    procps \
  && /var/lib/dpkg/info/ca-certificates-java.postinst configure \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
