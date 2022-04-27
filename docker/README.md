# Docker images for Glow and Hail

As of this time the following are supported: 

* Glow 1.2.1 + connectors to Azure Data Lake, Google Cloud Storage, Amazon Web Services (S3), Snowflake and Delta Lake (via data mechanics' Spark 3.2 Image) 
* Glow 1.2.1 + Databricks Runtime (DBR) 10.4 (Spark 3.2) + Ganglia
* Hail 0.2.93 + DBR 10.4 (Spark 3.2)

The containers are hosted on the [projectglow dockerhub](https://hub.docker.com/u/projectglow), 
Please see the Glow [Getting Started](https://glow.readthedocs.io/en/latest/getting-started.html) guide for documentation on how to use the containers.

## Building the containers

##### Troubleshooting

If you get the following error:

```
failed to solve with frontend dockerfile.v0: failed to create LLB definition: docker.io/projectglow/minimal:10.4: not found
```

please run this from the shell and try again:

```
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0
```

Please see this [stack overflow post](https://stackoverflow.com/questions/64221861/an-error-failed-to-solve-with-frontend-dockerfile-v0) for explanation.

Important: Docker builds may run out of memory, please increase
Docker's default memory setting, which is 2.0 GB, via Docker Desktop -> Preferences -> Resources -> Advanced.
Also, the build can take many hours, the limiting step is the installation of R packages.

To learn more about contributing to these images, please review the Glow [contributing guide](https://glow.readthedocs.io/en/latest/contributing.html#add-libraries-to-the-glow-docker-environment)

### Glow Docker layer architecture

The foundation layers are specific to setting the environment up in Databricks.
Ganglia is an optional layer for monitoring cluster metrics such as CPU load.

![Docker layer architecture](../static/glow_genomics_docker_image_architecture.png?raw=true "Glow Docker layer architecture")

The open source version of this architecture to run outside of Databricks is simpler, 
with a base layer that pulls from data mechanics' Spark Image, followed by the ```genomics``` and ```genomics-with-glow``` layers.

### Build the docker images as follows:

run ```docker/databricks/build.sh``` or ```docker/open-source-glow/build.sh``` to build all of the layers. 

To build any layer individually, change directory into the layer and run: 

```docker build <layer> -t <docker_repository>/<layer>:<tag>```

to publish a layer, please push it to your docker repository:

```docker push <docker_repository>/<layer>:<tag>```

For example, to build and push the first foundational layer run,

##### ProjetGlow minimal 
```cd dbr/dbr10.4```

```docker build minimal/ -t projectglow/minimal:10.4```

```docker push projectglow/minimal:10.4```

#### Directory structure
```
docker
├── README.md
└── databricks
    ├── build.sh
    └── dbr
        └── dbr10.4
            ├── dbfsfuse
            │   └── Dockerfile
            ├── ganglia
                │   ├── Dockerfile
                │   ├── ganglia
                │   │   ├── remove_old_ganglia_rrds.sh
                │   │   ├── save_snapshot.js
                │   │   ├── save_snapshot.sh
                │   │   ├── start_ganglia
                │   │   └── start_ganglia.sh
                │   ├── ganglia-monitor-not-active
                │   ├── ganglia.conf
                │   ├── gconf
                │   │   ├── conf.d
                │   │   │   └── modpython.conf
                │   │   ├── databricks-gmond.conf
                │   │   ├── gmetad.conf
                │   │   └── gmond.conf
                │   ├── gmetad-not-active
                │   ├── monit
                │   ├── spark-slave-not-active
                │   └── start_spark_slave.sh
            ├── genomics
            │   └── Dockerfile
            ├── genomics-with-glow
            │   └── Dockerfile
            ├── genomics-with-hail
            │   └── Dockerfile
            ├── minimal
            │   └── Dockerfile
            ├── python
            │   └── Dockerfile
            ├── r
            │   ├── Dockerfile
            │   └── Rprofile.site
            └── standard
                └── Dockerfile
```

#### Databricks Container Services (DCS) FAQ for this Docker project

##### Python dependencies

For sanity's sake, we've written our own Dockerfiles for Python for Databricks. This means Python is installed to ```/databricks/python3```. ```/databricks/python3/bin/python and /databricks/python3/bin/pip``` can thus be reliably used in your Dockerfile. 

One *must* predicate the runtime path for any images based on these Dockerfile as ```PATH=/databricks/python3/bin:$PATH```. This is required for any builds requiring Python. 

The Python version used *must* be compatible with the Databricks Runtime (DBR) version. Python 3.7 is compatible with DBR 7.3 LTS; Python 3.8 is compatible with DBR 10.4; and so on. Please check the (https://docs.databricks.com/release-notes/runtime/releases.html)[Databricks Runtime Release Notes] for details on compatibility. 

##### Scala dependencies

Jars are *best* installed using ```curl``` or ```wget```. These *must* be deployed to ```/databricks/jars```.
As tempting as it may be, installation using maven is not recommended.     

##### Spark configurations 

Configs need to be written to ```/databricks/driver/conf``` and predicated to load first upon cluster start; 
e.g. ```/databricks/driver/conf/00-hail-spark-driver-defaults.conf```

##### Databricks cluster init scripts

Write Databricks cluster init scripts to ```/databricks/scripts/```. When multiple init scripts are deployed, use predication to manage the order of initialisation where multiple init scripts are deployed.

##### Ganglia enablement on Databricks Container Services

Review the Dockerfile and configurations in the ```ganglia``` directory to see how to deploy ganglia in a way that enables metrics collection on Databricks clusters using Container Services. 

NOTE: ganglia is *not* officially supported on Databricks Container Services by Databricks. The ```ganglia``` docker image here is supported by the community on a *best efforts* basis, only.
