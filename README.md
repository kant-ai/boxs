# Boxs

 [![pipeline status](https://gitlab.com/kantai/boxs/badges/mainline/pipeline.svg)](https://gitlab.com/kantai/boxs/-/commits/mainline)
 [![coverage report](https://gitlab.com/kantai/boxs/badges/mainline/coverage.svg)](https://gitlab.com/kantai/boxs/-/commits/mainline)

Boxs is a python library that manages data automatically and keeps track of different
versions of the same data created in different runs of the same script. No more need to manually
think about file paths and S3 keys, just store the data and let boxs manage the rest.
Besides managing the version history of the data, boxs allows to track the dependencies between
different artifacts. It is meant as a tool for making it easy to manage artifacts in workflows
for data science and machine learning.

## What it does

Boxs provides simple functions that allow storing arbitrary values and loading them at
a later point. Instead of passing around file paths or s3 keys, that have to be manually
defined and versioned, boxs uses references to data, that are automatically generated
when data is initially stored.

The data items of every run of the python scripts are stored next to each other without
overwriting anything, which allows inspecting and comparing artifacts between different
runs. For easier usage, users can name individual data items or runs so that they can
be referred to by a simple name.

Additionally, boxs can automatically create meta-data for each stored data item. This
meta-data like data type, size, number of lines or checksum can be accessed during
execution and makes it easier to optimize the data handling.

## How it works

Boxs organizes the data it manages in so-called `Box`es, namespaces that allow to group
related data together. Boxes themselves don't store the data, but use different `Storage`
implementations to actually store the data and their meta-data. Within a storage a data
item is stored with 3 different identifiers:

- box_id: The id of the box, that this data item belongs to.
- data_id: An identifier for the data item, that is derived from where the data was
  created. This identifier is the same across multiple runs.
- run_id: The id of the run during which the data item was created. At the beginning
  of the python interpreter, a new random run_id is generated.

## How to use it

Boxs can be easily installed from PyPI using pip:

```bash
pip install tox
```

The first step is to define a box and the underlying `Storage`, which can be used for
storing the data:

```python
import boxs

storage = boxs.FileSystemStorage('/my/storage/directory')
box = boxs.Box('my-box-id', storage)
```

The API of boxs is quite simple and consists of mainly 3 different functions:

```python
data = boxs.store(value, *parents, name=None, box=box)

print(boxs.info(data))

value = boxs.load(data)
```

For more information, please take a look at
[Getting started](https://docs.kant.ai/boxs/latest/getting_started/) or the
[User guide](https://docs.kant.ai/boxs/latest/user_guide/).

## Develop

Boxs uses [tox](https://tox.wiki/en/latest/index.html) to build and test the library.
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
envlist = py36,py37,py38,py39
```
to contain only the python version you want to use. Another option is to run tox with
the additional command line argument
['--skip_missing_interpreters'](https://tox.wiki/en/latest/config.html#conf-skip_missing_interpreters)
which skips python versions that aren't installed.


## Documentation

The latest version of the documentation can always be found at
[https://docs.kant.ai/boxs/latest](https://docs.kant.ai/boxs/latest).
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
