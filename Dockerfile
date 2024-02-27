FROM databricksruntime/standard:14.3-LTS

ENV DEBIAN_FRONTEND noninteractive

# ===== Set up python environment ==================================================================

RUN /databricks/python3/bin/pip install awscli databricks-cli --no-cache-dir

# ===== Set up Azure CLI =====

RUN apt-get update && apt-get install -y \
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

ENV SAMTOOLS_VERSION=1.9

WORKDIR /opt
RUN wget https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2 && \
    tar -xjf samtools-1.9.tar.bz2
WORKDIR samtools-1.9
RUN ./configure --without-curses && \
    make && \
    make install

ENV PATH=${DEST_DIR}/samtools-{$SAMTOOLS_VERSION}:$PATH


# ===== Set up htslib ==============================================================================
# access htslib tools from the shell, for example,
# %sh 
# /opt/htslib-1.9/tabix
# /opt/htslib-1.9/bgzip

WORKDIR /opt
RUN wget https://github.com/samtools/htslib/releases/download/${SAMTOOLS_VERSION}/htslib-${SAMTOOLS_VERSION}.tar.bz2 && \
    tar -xjvf htslib-1.9.tar.bz2
WORKDIR htslib-1.9
RUN ./configure --without-curses && \
    make && \
    make install

# ===== Set up MLR dependencies ====================================================================

ENV QQMAN_VERSION=1.0.6
RUN /databricks/python3/bin/pip install qqman==$QQMAN_VERSION

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

# ===== Set up liftOver (used by standard Glow examples) ===========================================

RUN mkdir /opt/liftover
RUN curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain

# ===== Set up bedtools as desired by many Glow users ==============================================

ENV BEDTOOLS_VERSION=2.30.0
ENV PATH=/databricks/python3/bin:$PATH
RUN cd /opt && git clone --depth 1 --branch v${BEDTOOLS_VERSION} https://github.com/arq5x/bedtools2.git bedtools-${BEDTOOLS_VERSION} 
RUN cd /opt/bedtools-${BEDTOOLS_VERSION} && make 

# Install Glow
RUN mkdir /databricks/jars
RUN curl --output-dir /databricks/jars https://github.com/projectglow/glow/releases/download/v2.0.0/glow-2.0.0.jar
RUN curl https://github.com/projectglow/glow/releases/download/v2.0.0/glow-2.0.0-py3.whl -o glow.py-2.0.0-py3-none-any.whl && /databricks/python3/bin/pip install glow.py-2.0.0-py3-none-any.whl
