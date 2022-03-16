# Docker images for Glow and Hail

As of this time the following are supported: 

* Glow 1.1.2 + Databricks Runtime (DBR) 9.1 (Spark 3.1)
* Hail 0.2.78 + DBR 9.1 (Spark 3.1)

These Dockerfiles are built to run on Databricks, 
but can be adapted to run Glow & Hail in the open source,

Note: Docker builds may run out of memory, please increase
Docker's default memorry setting, which is 2.0 GB, via Preferences -> Resources -> Advanced.

### Build the docker images as follows:

#### Base images for Glow & Hail

##### ProjetGlow minimal 
```cd dbr/dbr9.1```
```docker build minimal/ -t projectglow/minimal:9.1```

##### ProjectGlow python 
```cd dbr/dbr9.1```
```docker build python/ -t projectglow/python:9.1```

##### ProjectGlow dbfsfuse 
```cd dbr/dbr9.1```
```docker build dbfsfuse/ -t projectglow/dbfsfuse:9.1```

##### ProjectGlow standard 
```cd dbr/dbr9.1```
```docker build python/ -t projectglow/standard:9.1```

##### ProjectGlow r 
```cd dbr/dbr9.1```
```docker build r/ -t projectglow/with-r:9.1```

##### ProjectGlow genomics base 
```cd dbr/dbr9.1```
```docker build genomics/ -t projectglow/genomics:9.1```

##### Glow (requires Spark 3.1 / DBR 9.x)
```cd dbr/dbr9.1```
```docker build genomics-with-glow/ -t projectglow/databricks-glow-minus-ganglia:<dbr_version>```

##### Glow-with-Ganglia
```cd dbr/dbr9.1```
```docker build ganglia/ -t projectglow/databricks-glow:<dbr_version>```

#### Hail image (requires Spark 3.1 / DBR 9.x) 
```cd dbr/dbr9.1```
```docker build genomics-with-hail/ -t projectglow/databricks-hail:<hail_version>```

#### Glow Docker layer architecture

The foundation layers are specific to setting the environment up in Databricks.
Ganglia is an optional layer for monitoring cluster metrics such as CPU load.

![Docker layer architecture](../static/glow_genomics_docker_image_architecture.png?raw=true "Glow Docker layer architecture")

#### Directory structure
```
docker
├── README.md
└── databricks
    ├── build.sh
    └── dbr
        └── dbr9.1
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

#### DCS FAQ for this Docker project

##### Python dependencies

For sanity's sake, we've written our own Dockerfiles for Python for Databricks. This means Python is installed to ```/databricks/python3```. ```/databricks/python3/bin/python and /databricks/python3/bin/pip``` can thus be reliably used in your Dockerfile. 

One *must* predicate the runtime path for any images based on these Dockerfile as ```PATH=/databricks/python3/bin:$PATH```. This is required for any builds requiring Python. 

The Python version used *must* be compatible with the Databricks Runtime (DBR) version. Python 3.7 is compatible with DBR 7.3 LTS; Python 3.8 is compatible with DBR 9.1; and so on. Please check the (https://docs.databricks.com/release-notes/runtime/releases.html)[Databricks Runtime Release Notes] for details on compatibility. 

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






