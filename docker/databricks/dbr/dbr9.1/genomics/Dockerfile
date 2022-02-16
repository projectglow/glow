FROM projectglow/with-r:9.1 AS r

# ===== Build off Databricks Runtime ===============================================================

#The runtime base is Ubuntu 18.04, or 20.04 after 9.x
#See more here https://github.com/databricks/containers

# ===== Set up python environment ==================================================================

RUN /databricks/python3/bin/pip install awscli databricks-cli --no-cache-dir

# ===== Set up Azure CLI =====

RUN apt-get install -y \
    curl \
    lsb-release \
    gnupg

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

# ===== Set up R genomics packages =================================================================

RUN R -e "install.packages('sim1000G',dependencies=TRUE,repos='https://cran.rstudio.com')"\
 && R -e "install.packages('ukbtools',dependencies=TRUE,repos='https://cran.rstudio.com')"\
 && R -e "install.packages('qqman',dependencies=TRUE,repos='http://cran.us.r-project.org')"\
 && R -e "install.packages('bigsnpr',dependencies=TRUE,repos='http://cran.us.r-project.org')"

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
RUN ./configure && \
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

# ===== Reset current directory ====================================================================

WORKDIR /root 

