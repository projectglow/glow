### To build the Docker images: 
* First build the Databricks R docker image:
    - ```cd ./r/```
    - ```docker build  -t databricksruntime/r-databricks:8.1 .```
* Then build the base Databricks genomics docker image: 
    - ```cd ..```
    - ```docker build - < ./genomics.azure.dockerfile -t databricksruntime/genomics-databricks-azure:8.x```
* Then build the Glow and Hail docker images
    - ```cd ./spark3.0```
    - ```docker build - < ./glow.dockerfile -t projectglow/glow-databricks:1.0.1```
    - ```cd ..```
    - ```cd ./spark3.1```
    - ```docker build - < ./glow.dockerfile -t projectglow/glow-databricks:1.1.0```
    - ```docker build - < ./hail.dockerfile -t hailgenetics/hail-databricks:0.2.74```
* Tag these as needed and push to your Docker repository

### Important things to consider when building on or modifying these Dockerfiles
* Any custom Spark configs must be written to ```/databricks/driver/conf/``` with a precedence prefix (e.g. ```00-my-custom-config.conf```)
  * No need to create init script to build any such config files - this is exceedingly complicated
    One can simply define the following in the Dockerfile
    ```
    SHELL ["/bin/bash", "-c"]
    RUN mkdir -p /databricks/driver/conf
    RUN echo -e '\
        [driver] {\n\
            "spark.kryo.registrator" = "is.hail.kryo.HailKryoRegistrator"\n\
            "spark.hadoop.fs.s3a.connection.maximum" = 5000\n\
            "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"\n\
        }\n\
    ' > /databricks/driver/conf/00-hail-spark-driver-defaults.conf
    ```

* Any custom jars to be packaged with the image must be deployed to ```/databricks/jars``` 
* Any PIP installs to be packaged with the image must use ```/databricks/conda/envs/dcs-minimal/bin/pip```
* Any python execution as part of image packaging must use ```/databricks/conda/envs/dcs-minimal/bin/python``` 
  * To force this, set ```ENV PATH=/databricks/conda/envs/dcs-minimal/bin/:$PATH``` prior to any builds requiring python
* Check dependencies on any and all builds and installs; include the necessary dependencies as needed preceding builds
  * e.g. ```apt-get update && apt-get install -y libxml2 libxml2-dev``` needs to be run before ```RUN R -e "install.packages('ukbtools',dependencies=TRUE,repos='https://cran.rstudio.com')```
* 