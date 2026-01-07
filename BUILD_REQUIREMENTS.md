# Glow Build Requirements

This document lists all packages and requirements needed to build Glow artifacts (Scala JAR and Python wheel).

## System Requirements

### Required Tools
1. **Java Development Kit (JDK) 8**
   - Required for Spark 3.x builds
   - For Spark 4.x: Java 17

2. **Scala Build Tool (sbt)**
   - Version: `1.10.4` (specified in `project/build.properties`)
   - Install from: https://www.scala-sbt.org/1.0/docs/Setup.html

3. **Conda**
   - Required for Python environment management
   - Install from: https://docs.conda.io/en/latest/miniconda.html

4. **Git**
   - For version control and cloning the repository

### Default Build Versions
- **Scala**: 2.12.19 (Spark 3.x) or 2.13.14 (Spark 4.x)
- **Spark**: 3.5.1 (default) or 4.0.0-SNAPSHOT
- **Python**: 3.10.12

> **Note**: Spark and Scala versions can be overridden using environment variables:
> - `SPARK_VERSION` - Set desired Spark version
> - `SCALA_VERSION` - Set desired Scala version

---

## Python Environment (Conda)

### Conda Packages (from conda-forge/bioconda)

#### Core Dependencies
- `python=3.10.12`
- `pip=22.3.1`

#### Scientific Computing
- `numpy=1.23.5`
- `pandas=1.5.3`
- `scipy=1.10.0`
- `scikit-learn=1.1.1`
- `statsmodels=0.13.5`
- `opt_einsum>=3.2.0`
- `nptyping`
- `typeguard`

#### Data Processing
- `pyarrow=8.0.1` (compatible with Databricks Runtime 14.2)

#### Testing
- `pytest=7.4.4`
- `pytest-cov=4.1.0`

#### Development Tools
- `jupyterlab`
- `yapf=0.40.1` (code formatting)
- `pygments=2.17.2` (syntax highlighting)

#### Bioinformatics
- `bedtools` (from bioconda channel)

#### Utilities
- `click=8.0.4` (CLI tool, for docs generation)
- `jinja2=3.1.2` (templating)
- `pyyaml` (YAML parsing)

### Python Packages (via pip)

#### Spark
- `pyspark==3.5.1` (or version matching SPARK_VERSION)

#### Databricks
- `databricks-cli==0.18` (docs notebook generation)
- `databricks-sdk` (latest version, for build script)

#### Build & Packaging
- `setuptools==65.6.3` (Python packaging)
- `twine` (PyPI publishing)

#### Documentation
- `sphinx` (documentation generator)
- `sphinx_rtd_theme` (Read the Docs theme)
- `sphinx-autobuild` (auto-rebuild docs)
- `sphinx-prompt` (command prompt styling)
- `Sphinx-Substitution-Extensions` (substitutions in code blocks)
- `sphinx-tabs` (code tabs for Python/Scala)
- `sybil>=6.0.0` (automatic doctest, requires version 6.0+ for pytest 7.4+ compatibility)

---

## Scala/SBT Dependencies

### SBT Plugins (from `project/plugins.sbt`)
- `sbt-assembly` 2.3.0 - Create fat JARs
- `sbt-sonatype` 3.12.2 - Maven Central publishing
- `sbt-pgp` 2.3.0 - PGP signing
- `sbt-scalafmt` 2.5.2 - Code formatting
- `scalastyle-sbt-plugin` 1.0.0 - Code style checking
- `sbt-scoverage` 2.2.2 - Code coverage
- `sbt-header` 5.10.0 - License header management

### Test Dependencies
- `scalatest` 3.2.18 - Scala testing framework

### Spark Dependencies
Automatically resolved by sbt based on `SPARK_VERSION`:
- Apache Spark SQL
- Apache Spark Core
- Apache Spark MLlib

---

## Runtime Requirements for `bin/build` Script

When using the `bin/build` script to build and optionally install artifacts on Databricks:

### Required Python Packages
- `databricks-sdk` - For uploading to Databricks clusters
- All packages from `python/environment.yml`

### Databricks Authentication
One of the following methods (see Databricks unified authentication):
- Environment variables: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- Configuration file: `~/.databrickscfg` with profile settings
- OAuth or other authentication methods supported by Databricks SDK

---

## Build Commands

### Setup Python Environment
```bash
# Create conda environment
conda env create -f python/environment.yml

# Activate environment
conda activate glow

# Update environment (if yml file changes)
conda env update -f python/environment.yml
```

### Build Scala JAR
```bash
# Using sbt directly
sbt core/assembly

# Using build script
bin/build --scala
```

### Build Python Wheel
```bash
# Using build script (recommended)
bin/build --python

# Or manually
cd python
python setup.py bdist_wheel
```

### Build Both Artifacts
```bash
bin/build --scala --python
```

### Build and Install on Databricks
```bash
# Install to DBFS (default)
bin/build --scala --python --install CLUSTER_ID

# Install to Unity Catalog Volume
bin/build --scala --python --install CLUSTER_ID --upload-to /Volumes/catalog/schema/volume
```

---

## Optional: Spark 4 Environment

For testing with Spark 4.0, use the alternative environment file:

```bash
conda env create -f python/spark-4-environment.yml
conda activate glow-spark4
```

Key differences:
- Python: 3.10.12
- PyArrow: 14.0.2 (newer version)
- PySpark: 3.5.1 (uninstalled before testing, using source from Spark git repo)
- Same testing and documentation tools

---

## Minimum Requirements Summary

To build Glow artifacts, you minimally need:

1. **Java 8** (or Java 17 for Spark 4)
2. **sbt 1.10.4**
3. **Conda** with the glow environment activated
4. **Git** (for cloning the repository)

Optional for Databricks deployment:
5. **Databricks SDK** and authentication configured
6. **Active Databricks cluster** (for `--install` option)

---

## Verification

To verify your environment is set up correctly:

```bash
# Check Java version
java -version  # Should show 1.8.x (or 17 for Spark 4)

# Check sbt version
sbt --version  # Should show 1.10.4

# Check conda environment
conda activate glow
python --version  # Should show Python 3.10.12

# Check key packages
python -c "import pyspark; print(pyspark.__version__)"
python -c "from databricks.sdk import WorkspaceClient; print('✓ Databricks SDK installed')"

# Verify sbt can compile
sbt compile
```

---

## Troubleshooting

### Common Issues

1. **sbt not found**: Install sbt from https://www.scala-sbt.org/download.html
2. **Java version mismatch**: Set `JAVA_HOME` to point to JDK 8 (or 17 for Spark 4)
3. **Conda environment issues**: Delete and recreate: `conda env remove -n glow && conda env create -f python/environment.yml`
4. **Import errors**: Ensure conda environment is activated: `conda activate glow`
5. **Databricks SDK import error**: The build script requires `databricks-sdk` which should be in environment.yml

---

For more details, see:
- Main README: [README.md](README.md)
- Release process: [RELEASE.md](RELEASE.md)
- Contributing guide: [CONTRIBUTING.md](CONTRIBUTING.md)

