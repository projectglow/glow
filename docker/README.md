# Docker images for Glow and Hail on Databricks

As of this time the following are supported: 

* Glow 1.1.0 + Databricks Runtime (DBR) 9.0 (Spark 3.1)
* Hail 0.2.76 + DBR 9.0 (Spark 3.1)

### Build the docker images as follows:

#### Base images for Glow & Hail

##### ProjetGlow minimal 
```cd dbr/dbr9.0```
```docker build minimal/ -t projectglow/minimal:9.0```

##### ProjectGlow python 
```cd dbr/dbr9.0```
```docker build python/ -t projectglow/python:9.0```

##### ProjectGlow dbfsfuse 
```cd dbr/dbr9.0```
```docker build dbfsfuse/ -t projectglow/dbfsfuse:9.0```

##### ProjectGlow standard 
```cd dbr/dbr9.0```
```docker build python/ -t projectglow/standard:9.0```

##### ProjectGlow r 
```cd dbr/dbr9.0```
```docker build r/ -t projectglow/with-r:9.0```

##### ProjectGlow genomics base 
```cd dbr/dbr9.0```
```docker build genomics/ -t projectglow/genomics:9.0```

##### Glow (requires Spark 3.1 / DBR 9.x)
```cd dbr/dbr9.0```
```docker build genomics-with-glow/ -t projectglow/databricks-glow:<dbr_version>```

#### Hail image (requires Spark 3.1 / DBR 9.x) 
```cd dbr/dbr9.0```
```docker build genomics-with-hail/ -t projectglow/databricks-hail:<hail_version>```

#### Directory structure
```
docker
├── README.md
└── databricks
    ├── build.sh
    └── dbr
        └── dbr9.0
            ├── dbfsfuse
            │   └── Dockerfile
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

The Python version used *must* be compatible with the Databricks Runtime (DBR) version. Python 3.7 is compatible with DBR 7.3 LTS; Python 3.8 is compatible with DBR 9.0; and so on. Please check the (https://docs.databricks.com/release-notes/runtime/releases.html)[Databricks Runtime Release Notes] for details on compatibility. 

##### Scala dependencies

Jars are best installed using ```curl``` or ```wget```. These *must* be deployed to ```/databricks/jars```.
As tempting as it may be, installation using maven is not recommended.     

##### Spark configurations 

Configs need to be written to ```/databricks/driver/conf``` and predicated to load first upon cluster start; 
e.g. ```/databricks/driver/conf/00-hail-spark-driver-defaults.conf```






