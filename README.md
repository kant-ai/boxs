# Datastock

 [![pipeline status](https://gitlab.com/kantai/datastock/badges/mainline/pipeline.svg)](https://gitlab.com/kantai/datastock/-/commits/mainline)
 [![coverage report](https://gitlab.com/kantai/datastock/badges/mainline/coverage.svg)](https://gitlab.com/kantai/datastock/-/commits/mainline)

Datastock is a python library that manages data automatically and keeps track of different
versions of the same data created in different runs of the same script. It is meant of as
a tool for implementing workflows in data science and machine learning.

## What it does



## How it works


## Develop

Datastock uses [tox](https://tox.wiki/en/latest/index.html) to build and test the library.
Tox runs all tests on different python versions, can generate the documentation and run
linters and style checks to improve the code quality.
In order to install all the necessary python modules, please run:

```bash
pip install tox
```

Afterwards the tests can be run by just calling

```bash
tox
```

from the project directory. For this to work, you need to have multiple python
interpreters installed. If you don't want to run the tests on all supported platforms
just edit the tox.ini file and set
```
envlist = py36,py37,py38
```
to contain only the python version you want to use. Another option is to run tox with
the additional command line argument
['--skip_missing_interpreters'](https://tox.wiki/en/latest/config.html#conf-skip_missing_interpreters)
which skips python versions that aren't installed.


## Documentation

The latest version of the documentation can always be found under https://docs.kant.ai/datastock/latest.
The documentation is written in [Markdown](https://daringfireball.net/projects/markdown/)
and is located in the `docs` directory of the project.
It can be built into static HTML by using [MkDocs](https://www.mkdocs.org/).
In order to manually generate the documentation we can use tox to build the HTML pages from our markdown.

```bash
tox -e docs
```

## Release

### Releasing a new package version

Releasing new versions of bandsaw is done using [flit](https://flit.readthedocs.io/en/latest/).

```bash
pip install flit
```

In order to be able to publish a new release, you need an account with PyPI or their
respective test environment.

Add those accounts into your `~.pypirc`:
```
[distutils]
index-servers =
  pypi
  pypitest

[pypi]
username: <my-user>

[pypitest]
repository: https://test.pypi.org/legacy/
username: <my-test-user>
```


### Publishing a new release to test

```bash
flit publish --repository pypitest
```

### Releasing a new version of the documentation

The package uses [mike](https://github.com/jimporter/mike)
to manage multiple versions of the documentation. The already generated documentation is kept
in the `docs-deployment` branch and will be automatically deployed, if the branch is pushed to
the repository.

In order to build a new version of the documentation, we need to use the corresponding tox environment:

```bash
VERSION_TAG='<my-version>' tox -e docs-release
```

The `VERSION_TAG` environment variable should be set to the new version in format '<major>.<minor>'.
This will build the documentation and add it as new commits to the `docs-deployment` branch.

By pushing the updated branch to the gitlab repository, the documentation will be automatically
deployed to [the official documentation website](https://docs.kant.ai/datastock).
