[![pipeline status](https://gitlab.com/databand/dbnd/badges/master/pipeline.svg)](https://gitlab.com/databand/dbnd/commits/master) [![coverage report](https://gitlab.com/databand/dbnd/badges/master/coverage.svg)](https://gitlab.com/databand/dbnd/commits/master) 

![PyPI - Downloads](https://img.shields.io/pypi/dm/dbnd) ![PyPI](https://img.shields.io/pypi/v/dbnd) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbnd) ![PyPI - License](https://img.shields.io/pypi/l/dbnd) 
![Code style: ](https://img.shields.io/badge/code%20style-black-000000.svg)

# DBND Core 

DBND is an agile pipeline framework that helps date engineering teams track and orchestrate their data processes. Used for processes ranging from data prep, machine learning model training, experimentation, testing, and production, DBND is easy to use and natively provides teams visibility, reproducibility, and dynamic orchestration for your projects.

**DBND simplifies the process of building, running, and tracking data pipelines**
 
```python
@task
def say_hello(name: str = "databand.ai") -> str:
    value = "Hello %s!" % name
    return value
```

**And provides way for tracking your critical pipeline metadata**

```python
    log_dataframe("my_dataset", my_dataset) 
    log_metric("r2", r2)
```
 

### Getting Started
See our [Quickstart guide](https://databand.readme.io/docs/quickstart-1) to get up and running with DBND. Our [documentation](https://databand.readme.io/) is available for more examples and guides

### The Latest and Greatest
We recommend that you work with a virtual environment like [Virtualenv](https://virtualenv.pypa.io/en/latest/) or [Conda](https://docs.conda.io/en/latest/).
Updating to the latest and greatest:

```bash
pip install dbnd
```

If you would like access to our latest features, or have any questions, feedback, or contributions we would love to here from you! Get in touch through contact@databand.ai
