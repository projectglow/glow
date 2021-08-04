# ===== For the runtime environment for this image we need the databricks azure setup ============== 
FROM databricksruntime/genomics-azure:8.x

# ===== Install python dependencies for Glow =======================================================
# Upgrade to Glow 1.1.0 when available
# ENV GLOW_VERSION=1.1.0
# once available, we want specify that the earliest version is 1.1.0

ENV GLOW_VERSION=1.0.1

# RUN /databricks/conda/bin/pip3 install - glow.py==$GLOW_VERSION
RUN /databricks/conda/envs/dcs-minimal/bin/pip install glow.py==$GLOW_VERSION

# ===== Set up scala dependencies for Glow =========================================================
ENV GLOW_VERSION=1.0.1

RUN mkdir /databricks/jars
RUN cd /databricks/jars && curl -O \
https://search.maven.org/remotecontent?filepath=io/projectglow/glow-spark3_2.12/1.0.1/glow-spark3_2.12-${GLOW_VERSION}.jar
