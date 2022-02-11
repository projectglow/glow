# Images using this R layer also need Databricks' Python environment
FROM projectglow/standard:9.1

# Suppress interactive configuration prompts
ENV DEBIAN_FRONTEND=noninteractive

# Set Databricks Run Time Version, This variable used to perform a runtime version check to see whether we can use notebook-scoped libraries in R.
ENV DATABRICKS_RUNTIME_VERSION=9.1

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
