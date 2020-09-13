# Development Guide

## Make Patch or Release

### Prerequisite:

```shell script
pip install wheel twine bumpversion
```

### Create release

```shell script
./etc/scripts/dbnd-release patch|release
```

## Pre Commit hooks

dbnd.git has pre-commit hooks file. Run `pre-commit install` to install pre-commit into your git hooks. pre-commit will now run on every commit. Every time you clone a project using pre-commit running pre-commit install should always be the first thing you do.

If you want to manually run all pre-commit hooks on a repository, run `pre-commit run --all-files`. To run individual hooks use `pre-commit run <hook_id>`.

The first time pre-commit runs on a file it will automatically download, install, and run the hook. Note that running a hook for the first time may be slow. For example: If the machine does not have node installed, pre-commit will download and build a copy of node.

If you want to install pre-commit app on osx:

```shell script
brew install pre-commit
```

## Debug

### User Space Task and Configs objects build

```ini
[task_build]
verbose=True
```
