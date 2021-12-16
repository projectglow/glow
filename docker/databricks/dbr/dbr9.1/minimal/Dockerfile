FROM amd64/ubuntu:20.04

RUN apt-get update \
  && apt-get install --yes \
    ca-certificates-java=20190405ubuntu1 \
    openjdk-8-jdk \
    iproute2 \
    bash \
    sudo \
    coreutils \
    procps \
  && /var/lib/dpkg/info/ca-certificates-java.postinst configure \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
