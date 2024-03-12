# Releasing Glow

## High level workflow

The Glow release process is simple. We release by snapshotting the `main` branch rather than using release branches. So, the high level steps are:
- Cut a release tag from `main`
- Push Scala and Python artifacts to staging artifact repositories
- Perform any QA not covered by CI
- Promote artifacts from staging
- Create a release page on GitHub

## Cut a release tag

This step is automated by a [GitHub action](https://github.com/projectglow/glow/actions/workflows/cut-release.yml). You must input the version you wish to release as well as the next planned development version. *These version numbers should not include a `v` prefix.*

The action will automatically open a pull request against `main` to increment the development version number. Due to limitations in GitHub actions, tests will not run automatically against this pull request. You must close and reopen the pull request to trigger tests.
The tag created by the action will always begin with `v`. For example, if the input version is `2.0.0`, the tag will be named `v2.0.0`.

## Push artifacts to staging

This step is automated by a [GithHub action](https://github.com/projectglow/glow/actions/workflows/staging-release.yml). The "Use workflow from" option should always be "main" to use the latest version of the workflow. Then input the Spark, Scala, and Java versions you want to build against. The job will push Scala artifacts to a Sonatype staging repository and Python artifacts to Test PyPI. Tests run before pushing and then again on the staged Scala artifact.

### Cross building

To build artifacts for multiple Scala, Spark, or Java versions, just run the action multiple times. We typically do not build multiple Python artifacts, so the Test PyPI deployment will fail after the first push. To avoid this error, uncheck the option to push Python artifacts for all but one run.

## Perform any QA not covered by CI

The requirements of this step may change over time, so it's best to check with someone from Databricks about what QA steps you should take. However, you should always try installing the staged artifacts on a Databricks cluster to make sure that everything works.

### Scala artifact

Install the Scala artifact as a Maven library. To find the repository id, check the logs from the "Push to staging repositories" job and look for a line like "Created successfully: ioprojectglow-xxxx". For example, [this run](https://github.com/projectglow/glow/actions/runs/8244738235/job/22547455645) created a repository numbered `1043`.

Install the Maven library with parameters like:
Coordinates: `io.projectglow:glow-spark${SPARK_MAJOR_VERSION}_${SCALA_MAJOR_MINOR_VERSION}:${GLOW_VERSION}
Repository: `https://oss.sonatype.org/content/repositories/ioprojectglow-${SONATYPE_REPOSITORY_NUMBER}/`

### Python artifact

Install the Python artifact as a PyPI library with package `glow.py==${GLOW_VERSION}` and Index URL `https://test.pypi.org/simple/`.

If some of the dependencies are not in Test PyPI, you'll have to install them from main PyPI. There are several ways to accomplish this, but the simplest way is to use an init script with contents like:
```
/databricks/python/bin/pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple glow.py==${GLOW_VERSION}
```

## Promote artifacts from staging

This step is automated by a [GitHub Action](https://github.com/projectglow/glow/actions/workflows/production-release.yml). The parameters are similar to the staging action, but you don't need to choose a Scala version since the artifacts are already built. The Spark version is only necessary to know the Glow artifact name. Again, for cross building, run the action multiple times and deselect the option to push Python artifacts for all but one.

### Conda release

The Conda release is not automated. After releasing to PyPI, open a pull request against the [Glow feedstock](https://github.com/conda-forge/glow-feedstock). You can use [this pull request](https://github.com/conda-forge/glow-feedstock/pull/8) as a template. You can find the source sha256 on PyPI. The version numbers in `meta.yml` should match `python/setup.py`.