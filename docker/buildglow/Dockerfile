FROM amd64/ubuntu:20.04

# ==================================================================================================
# minimal
# ==================================================================================================

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
# Suppress interactive configuration prompts

# ==================================================================================================
# python 
# ==================================================================================================

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
# except: 
# downgrade ipython to maintain backwards compatibility with 7.x and 8.x runtimes
# downgrade numpy to avoid issue here: https://stackoverflow.com/questions/63761366/numpy-linalg-linalgerror-svd-did-not-converge-in-linear-least-squares-on-first
RUN /databricks/python3/bin/pip install \
  six==1.15.0 \
  ipython==7.22.0 \
  numpy==1.18.5 \
  pandas==1.2.4 \
  pyarrow==4.0.0 \
  matplotlib==3.4.2 \
  jinja2==3.0.3 \
  mlflow==1.19.0

ENV MLFLOW_TRACKING_URI=databricks

# Specifies where Spark will look for the python process
ENV PYSPARK_PYTHON=/databricks/python3/bin/python3

# ==================================================================================================
# dbfsfuse
# ==================================================================================================

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

# ==================================================================================================
# standard 
# ==================================================================================================

RUN apt-get update \
  && apt-get install -y openssh-server \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Warning: the created user has root permissions inside the container
# Warning: you still need to start the ssh process with `sudo service ssh start`
RUN useradd --create-home --shell /bin/bash --groups sudo ubuntu

# ==================================================================================================
# with-r
# ==================================================================================================

# Suppress interactive configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Set Databricks Run Time Version, This variable used to perform a runtime version check to see whether we can use notebook-scoped libraries in R.
ENV DATABRICKS_RUNTIME_VERSION=10.4

# update indices
# add the signing key (by Michael Rutter) for these repos
# To verify key, run gpg --show-keys /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc 
# Fingerprint: 298A3A825C0D65DFD57CBB651716619E084DAB9
# https://cran.rstudio.com/bin/linux/ubuntu/#secure-apt
RUN apt update -qq \
  && apt-get install --yes \
    apt-utils \
    software-properties-common \
    apt-transport-https \
    dirmngr \
    libssl-dev \
    r-base \
    r-base-dev \
  && wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | sudo tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc \
  && add-apt-repository "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/" \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


# hwriterPlus is used by Databricks to display output in notebook cells
# Rserve allows Spark to communicate with a local R process to run R code
RUN R -e "install.packages(c('hwriterPlus'), repos='https://mran.revolutionanalytics.com/snapshot/2017-02-26')" \
 && R -e "install.packages(c('htmltools'), repos='https://cran.microsoft.com/')" \
 && R -e "install.packages('Rserve', repos='http://rforge.net/')"

# Additional instructions to setup rstudio. If you dont need rstudio, you can 
# omit the below commands in your docker file. Even after this you need to use
# an init script to start the RStudio daemon (See README.md for details.)

# Databricks configuration for RStudio sessions.
COPY Rprofile.site /usr/lib/R/etc/Rprofile.site

# Rstudio installation.
RUN apt-get update \
 # Installation of rstudio in databricks needs /usr/bin/python and gdebi-core.
 && apt-get install -y \
    python \
    gdebi-core \
    psmisc \
    wget \
 # Download rstudio server for ubuntu 20 and install it.
 # https://www.rstudio.com/products/rstudio/download-server/debian-ubuntu/
 && wget https://download2.rstudio.org/server/bionic/amd64/rstudio-server-2021.09.1-372-amd64.deb -O rstudio-server.deb \
 && gdebi rstudio-server.deb \
 && rm rstudio-server.deb \
 && rstudio-server restart || true

# ==================================================================================================
# genomics 
# ==================================================================================================

# ===== Set up python environment ==================================================================

RUN /databricks/python3/bin/pip install awscli databricks-cli --no-cache-dir

# ===== Set up Azure CLI =====

RUN apt-get install -y \
    curl \
    lsb-release \
    gnupg \
    tzdata

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# ===== Set up base required libraries =============================================================

RUN apt-get update && apt-get install -y \
    apt-utils \
    build-essential \
    git \
    apt-transport-https \
    ca-certificates \
    cpanminus \
    libpng-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    perl \
    perl-base \
    unzip \
    curl \
    gnupg2 \
    software-properties-common \
    jq \
    libjemalloc2 \
    libjemalloc-dev \
    libdbi-perl \
    libdbd-mysql-perl \
    libdbd-sqlite3-perl \
    zlib1g \
    zlib1g-dev \
    libxml2 \
    libxml2-dev 


# ===== Set up VEP environment =====================================================================

ENV OPT_SRC /opt/vep/src
ENV PERL5LIB $PERL5LIB:$OPT_SRC/ensembl-vep:$OPT_SRC/ensembl-vep/modules
RUN cpanm DBI && \
    cpanm Set::IntervalTree && \
    cpanm JSON && \
    cpanm Text::CSV && \
    cpanm Module::Build && \
    cpanm PerlIO::gzip && \
    cpanm IO::Uncompress::Gunzip

RUN mkdir -p $OPT_SRC
WORKDIR $OPT_SRC
RUN git clone https://github.com/Ensembl/ensembl-vep.git
WORKDIR ensembl-vep

# The commit is the most recent one on release branch 100 as of July 29, 2020

RUN git checkout 10932fab1e9c113e8e5d317e1f668413390344ac && \
    perl INSTALL.pl --NO_UPDATE -AUTO a && \
    perl INSTALL.pl -n -a p --PLUGINS AncestralAllele && \
    chmod +x vep

# ===== Set up samtools ============================================================================

ENV SAMTOOLS_VERSION=1.15.1

WORKDIR /opt
RUN wget https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2 && \
    tar -xjf samtools-${SAMTOOLS_VERSION}.tar.bz2
WORKDIR samtools-${SAMTOOLS_VERSION}
RUN ./configure && \
    make && \
    make install

ENV PATH=${DEST_DIR}/samtools-{$SAMTOOLS_VERSION}:$PATH


# ===== Set up htslib ==============================================================================
# access htslib tools from the shell, for example,
# %sh 
# /opt/htslib-${SAMTOOLS_VERSION}/tabix
# /opt/htslib-${SAMTOOLS_VERSION}/bgzip

WORKDIR /opt
RUN wget https://github.com/samtools/htslib/releases/download/${SAMTOOLS_VERSION}/htslib-${SAMTOOLS_VERSION}.tar.bz2 && \
    tar -xjvf htslib-${SAMTOOLS_VERSION}.tar.bz2
WORKDIR htslib-${SAMTOOLS_VERSION}
RUN ./configure && \
    make && \
    make install

# ===== Set up bcftools ==============================================================================
# to access bcftools from the shell
# %sh
# /opt/bcftools-${SAMTOOLS_VERSION}/bcftools

WORKDIR /opt
RUN wget https://github.com/samtools/bcftools/releases/download/${SAMTOOLS_VERSION}/bcftools-${SAMTOOLS_VERSION}.tar.bz2 && \
    tar -xjvf bcftools-${SAMTOOLS_VERSION}.tar.bz2
WORKDIR bcftools-${SAMTOOLS_VERSION}
RUN ./configure && \
    make && \
    make install

# ===== bgenix ==============================================================================
#access begenix from the shell from,
#/opt/bgen/build/apps/bgenix

RUN apt-get update && apt-get install -y \
    npm

RUN npm install --save sqlite3

WORKDIR /opt
RUN wget http://code.enkre.net/bgen/tarball/release/bgen.tgz && \
    tar zxvf bgen.tgz && \
    mv bgen.tgz bgen
WORKDIR bgen
RUN CXX=/usr/bin/g++ && \
    CC=/usr/bin/gcc && \
    ./waf configure && \
    ./waf && \
    ./build/test/unit/test_bgen && \
    ./build/apps/bgenix -g example/example.16bits.bgen -list

# ===== Set up MLR dependencies ====================================================================

ENV QQMAN_VERSION=1.0.6
RUN /databricks/python3/bin/pip install qqman==$QQMAN_VERSION

# ===== Set up R genomics packages =================================================================

RUN R -e "install.packages('sim1000G',dependencies=TRUE,repos='https://cran.rstudio.com')"\
 && R -e "install.packages('gplots',dependencies=TRUE,repos='http://cran.us.r-project.org')"\
 && R -e "install.packages('bigsnpr',dependencies=TRUE,repos='http://cran.us.r-project.org')"\
 && R -e "install.packages('ukbtools',dependencies=TRUE,repos='https://cran.rstudio.com')"\
 && R -e "install.packages('qqman',dependencies=TRUE,repos='http://cran.us.r-project.org')"

# ===== plink ==============================================================================
#install both plink 1.07 and 1.9
#access plink from the shell from,
#v1.07
#/opt/plink-1.07-x86_64/plink --noweb
#v1.90
#/opt/plink --noweb

WORKDIR /opt
RUN wget http://zzz.bwh.harvard.edu/plink/dist/plink-1.07-x86_64.zip && \
    unzip plink-1.07-x86_64.zip
RUN wget http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20200616.zip && \
    unzip plink_linux_x86_64_20200616.zip

# ===== Reset current directory ====================================================================

WORKDIR /root

# ==================================================================================================
# genomics with glow 
# ==================================================================================================

ENV GLOW_VERSION=1.2.1

# ===== Install python dependencies for Glow =======================================================
# once available, we want specify that the earliest version is 1.1.0

RUN /databricks/python3/bin/pip install glow.py==$GLOW_VERSION

ENV BIOINFOKIT_VERSION=0.8.5
RUN /databricks/python3/bin/pip install bioinfokit==$BIOINFOKIT_VERSION

# ===== Set up scala dependencies for Glow =========================================================

ENV HADOOP_BAM_VERSION=7.9.1
ENV HTSJDK_VERSION=2.21.2
ENV PICARD_VERSION=2.23.3
ENV JDBI_VERSION=2.78

RUN mkdir /databricks/jars
RUN cd /databricks/jars && curl -O \
https://repo1.maven.org/maven2/io/projectglow/glow-spark3_2.12/${GLOW_VERSION}/glow-spark3_2.12-${GLOW_VERSION}.jar 
RUN cd /databricks/jars && curl -O \
https://repo1.maven.org/maven2/org/seqdoop/hadoop-bam/${HADOOP_BAM_VERSION}/hadoop-bam-${HADOOP_BAM_VERSION}.jar
RUN cd /databricks/jars && curl -O \ 
https://repo1.maven.org/maven2/com/github/samtools/htsjdk/${HTSJDK_VERSION}/htsjdk-${HTSJDK_VERSION}.jar
RUN cd /databricks/jars && curl -O \ 
https://repo1.maven.org/maven2/com/github/broadinstitute/picard/${PICARD_VERSION}/picard-${PICARD_VERSION}.jar
RUN cd /databricks/jars && curl -O \
https://repo1.maven.org/maven2/org/jdbi/jdbi/${JDBI_VERSION}/jdbi-${JDBI_VERSION}.jar

# ===== Set up needed Spark config for scala jars ==================================================

ENV JAVA_OPTS="-Dspark.executor.extraClassPath=/databricks/jars/glow-spark3_2.12-${GLOW_VERSION}.jar,/databricks/jars/hadoop-bam-${HADOOP_BAM_VERSION}.jar,/databricks/jars/htsjdk-${HTSJDK_VERSION}.jar,/databricks/jars/picard-${PICARD_VERSION}.jar,/databricks/jars/jdbi-${JDBI_VERSION}.jar \
               -Dspark.driver.extraClassPath=/databricks/jars/glow-spark3_2.12-${GLOW_VERSION}.jar,/databricks/jars/hadoop-bam-${HADOOP_BAM_VERSION}.jar,/databricks/jars/htsjdk-${HTSJDK_VERSION}.jar,/databricks/jars/picard-${PICARD_VERSION}.jar,/databricks/jars/jdbi-${JDBI_VERSION}.jar \
               -Dspark.serializer=org.apache.spark.serializer.KryoSerializer \
               -Dspark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec,org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec"

# ===== Set up liftOver (used by standard Glow examples) ===========================================

RUN mkdir /opt/liftover
RUN curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain

# ===== Set up bedtools as desired by many Glow users ==============================================

ENV BEDTOOLS_VERSION=2.30.0
ENV PATH=/databricks/conda/envs/dcs-minimal/bin/:$PATH
RUN cd /opt && git clone --depth 1 --branch v${BEDTOOLS_VERSION} https://github.com/arq5x/bedtools2.git bedtools-${BEDTOOLS_VERSION} 
RUN cd /opt/bedtools-${BEDTOOLS_VERSION} && make 

# Add bedtools path to the enviroment

ENV PATH=/opt/bedtools-${BEDTOOLS_VERSION}:$PATH 

WORKDIR /root/

# ==================================================================================================
# scala + sbt
# ==================================================================================================

RUN apt-get update
RUN apt-get install apt-transport-https curl gnupg -yqq
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
RUN chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
RUN apt-get update
RUN apt-get install sbt


