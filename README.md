[![pipeline status](https://gitlab.com/databand-ai/dbnd/badges/master/pipeline.svg)](https://gitlab.com/databand-ai/dbnd/pipelines)

![PyPI - Downloads](https://img.shields.io/pypi/dm/dbnd) ![PyPI](https://img.shields.io/pypi/v/dbnd)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbnd) ![PyPI - License](https://img.shields.io/pypi/l/dbnd)
![Code style: ](https://img.shields.io/badge/code%20style-black-000000.svg)

# DBND


DBND is an open-source framework for building and tracking data pipelines. It is used for processes ranging from data ingestion, preparation, machine learning model training, and production.

DBND includes a Python library, a set of APIs, and a CLI that enables you to collect metadata from your workflows, create a system of record for runs, and easily orchestrate complex processes.

## Project Overview

DBND simplifies the process of building and running data pipelines:

```python
from dbnd import task

@task
def say_hello(name: str = "databand.ai") -> str:
    value = "Hello %s!" % name
    return value
```

Additionally, DBND makes it easy to track your critical pipeline metadata:

```python
from dbnd import log_metric, log_dataframe

log_dataframe("my_dataset", my_dataset)
log_metric("r2", r2)
```

## Prerequisites

This project is built using Python. To ensure compatibility, use the recommended version of Python supported by DBND. Make sure you have the following installed before proceeding:

- [Virtualenv](https://virtualenv.pypa.io/en/latest/) or [Conda](https://docs.conda.io/en/latest/)

## Installation Steps

### Option 1: Using `pip`
To install DBND, run:
```shell
pip install dbnd
```

### Verification
To verify the successful installation, import the library and run a simple task:
```python
from dbnd import task
@task
def verify_installation() -> str:
    return "Installation successful!"
```

### Getting Started

See our [documentation](https://www.ibm.com/docs/en/dobd) with examples and quickstart guides to get up and running with DBND.

## Help and Support

If you have any questions, feedback, or contributions, feel free to get in touch with us at contact@databand.ai, and open pull request and issues in this repository.

## External Documents

Refer to additional documents for further guidance:
- [Virtualenv Documentation](https://virtualenv.pypa.io/en/latest/)
- [Conda Documentation](https://docs.conda.io/en/latest/)

## Version History

To stay updated with the latest features and improvements, ensure you update DBND regularly via `pip install --upgrade dbnd`.


