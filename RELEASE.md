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

## Push artifacts to staging

This step is automated by a [GithHub action]()
