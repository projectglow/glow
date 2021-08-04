# ===== Needed for maven install process only ======================================================
FROM maven:3.8.1-adoptopenjdk-8 AS maven 

# ===== Install scala dependencies for Glow ========================================================
ENV GLOW_VERSION=1.0.1

WORKDIR /root/
RUN mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get \
      -DrepoUrl=repo1.maven.org/maven2/ \
      -Dartifact=io.projectglow:glow-spark3_2.12:$GLOW_VERSION 

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

COPY --from=maven /root/.m2 /root/.m2
RUN glow_jar_path=$(eval "find /root/.m2  -name 'glow-spark3_2.12-${GLOW_VERSION}.jar'")
RUN mkdir /databricks/jars
COPY $glow_jar_path /databricks/jars

