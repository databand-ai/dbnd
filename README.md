[![pipeline status](https://gitlab.com/databand-ai/dbnd/badges/master/pipeline.svg)](https://gitlab.com/databand-ai/dbnd/pipelines)

![PyPI - Downloads](https://img.shields.io/pypi/dm/dbnd) ![PyPI](https://img.shields.io/pypi/v/dbnd)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbnd) ![PyPI - License](https://img.shields.io/pypi/l/dbnd)
![Code style: ](https://img.shields.io/badge/code%20style-black-000000.svg)

# DBND

DBND an open source framework for building and tracking data pipelines. DBND is used for processes ranging from data ingestion, preparation, machine learning model training and production.

DBND includes a Python library, set of APIs, and CLI that enables you to collect metadata from your workflows, create a system of record for runs, and easily orchestrate complex processes.

DBND simplifies the process of building and running data pipelines
from dbnd import task

```python
from dbnd import task

@task
def say_hello(name: str = "databand.ai") -> str:
    value = "Hello %s!" % name
    return value
```

And makes it easy to track your critical pipeline metadata

```python
from dbnd import log_metric, log_dataframe

log_dataframe("my_dataset", my_dataset)
log_metric("r2", r2)
```

## Getting Started

See our [documentation](https://www.ibm.com/docs/en/dobd) with examples and quickstart guides to get up and running with DBND.

## The Latest and Greatest

For using DBND, we recommend that you work with a virtual environment like [Virtualenv](https://virtualenv.pypa.io/en/latest/) or [Conda](https://docs.conda.io/en/latest/). Update to the latest and greatest:

```shell script
pip install dbnd
```

If you would like access to our latest features, or have any questions, feedback, or contributions we would love to here from you! Get in touch through contact@databand.ai
