# Development Guide

## VirtualEnv
There are 3 virtual environments (virtualenv) used for development
* core - main virtualenv for all tracking modules
* tracking-airflow - virtualenv used Apache Airflow tracking (development and testing)
* run-airflow - virutalenv used by orchestration components with Apache Airflow pre-installed

Every environment have a set of predefined commands to manage Virtualenv. You can call them using `make` by using prefix of the env ( `make tracking-airflow--..`, `run-airflow--..`)

Working with dbnd-core (py39 is recommended):
_(it's recommended to open dbnd-core as a separate project in your IDE)_

```bash
# create venv-dbnd-core-py39 (nedded once)
make create-venv
# activate dbnd-core venv
pyenv activate dbnd-py39
# install dbnd-core related modules and plugins
make install-dev
```

## Pre Commit hooks

dbnd.git has pre-commit hooks file. Run `pre-commit install` to install pre-commit into your git hooks. pre-commit will now run on every commit. Every time you clone a project using pre-commit running pre-commit install should always be the first thing you do.

If you want to manually run all pre-commit hooks on a repository, run `pre-commit run --all-files`. To run individual hooks use `pre-commit run <hook_id>`.

The first time pre-commit runs on a file it will automatically download, install, and run the hook. Note that running a hook for the first time may be slow. For example: If the machine does not have node installed, pre-commit will download and build a copy of node.

If you want to install pre-commit app on osx:

```shell script
brew install pre-commit
```

## Tests

### Run tests for dbnd sdk only with the default Python:
`make test`

### Run tests for all dbnd-core python packages with tox.
`make test-all-py39`

### Run minifest tests for all dbnd-core  python package with tox.
`make test-manifest`

### Debug specific test

`py.test modules/dbnd/test_dbnd -k SpecificKeywordOrClassName`

### Writing new test

Use dbnd-example only for customer-facing examples/documentation If you have a complex "testing" scenario used for
development/debugging, please use `dbnd-test-scnearios` package. This package is installed at dev environment
