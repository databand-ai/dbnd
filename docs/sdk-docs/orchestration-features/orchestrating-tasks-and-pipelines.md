---
"title": "Getting Started with Orchestration"
---
Databand's SDK ("DBND") enables data engineers to orchestrate dynamic pipelines. DBND is an advanced framework that allows you to manage your data flows and run them on supported orchestrators. You can use DBND by defining pipelines with DBND decorators and executing pipelines with DBND's runtime.

Pipeline frameworks today did not provide an easy abstraction layer that enabled engineers to focus cleanly on their business logic while maintaining high development agility - and DBND successfully solves this problem.

DBND provides the abstraction layer that enables you to:
* Decouple pipeline code from data & compute infrastructure, making it easier to iterate
* Enable adaptive/dynamic pipelines (so you don’t write as much hardcoded logic to define different pipeline behaviors)
* Track everything that you run, including performance metrics, data flow, and data quality information

Engineers should not have to worry so much about the surrounding noise, like passing data between tasks of the pipeline or running tasks on different compute engines. It slows down team agility by requiring more redundant code and overhead. Databand solves that with an agile framework for pipeline development.

## Data tasks and pipelines

With DBND, you can define data tasks and pipelines ("DAGs") at the function level which simplifies the process of building and iterating your pipelines.

The code snippet below is a Python function that takes some input parameters and returns two DataFrames. Using the `@task` decorator makes the function a task in DBND. The decorator also tells DBND to manage and track the function's inputs and outputs:

```python
from dbnd import task
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from typing import Tuple


# define a function with a decorator

@task
def prepare_data(raw_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
    train_df, validation_df = train_test_split(raw_data)

    return train_df, validation_df
```

### Decorators
Decorators are powerful. They offer a lot of capabilities in a small package. They act as function wrappers for your pipelines., You can write code the way you want to.

We can define a pipeline by wiring together any number of tasks:

```python
from dbnd import task, pipeline
from pandas import DataFrame

@task
def prepare_data(data: DataFrame, key: str) -> str:
    result = data.get(key)
    return result


@pipeline
def prepare_data_pipeline(raw_data: DataFrame):
    key = acquire_key(raw_data)
    treated_data = prepare_data(raw_data, key)

    return treated_data
```

When wiring pipelines with DBND, DBND will automate passing outputs between tasks, making it easier to iterate pipelines with new data files and parameters.

## Data portability

With DBND, you can manage single points of connection to data infrastructure. Since DBND takes care of passing outputs between tasks, you can centrally manage data set connections and run pipelines with new data files as needed.

Data portability is especially useful for pipeline development. You can easily inject specific inputs during debugging and testing. Through a simple CLI switch, you can run the same pipeline with a data file from Amazon S3 or a data file from your local file system.

```bash
$ dbnd run prepare_data --set prepare_data.raw_data=s3://my_backet/myfile.json
$ dbnd run prepare_data --set prepare_data.raw_data=datafile.csv
```

Using this approach, you can read and write data from any data source (local, AWS S3, GCP GS, Azure blob, HDFS, etc.); inject parameters and new data from environment, command line, and configuration providers/files.

For information about task configuration, see [Task Configuration](doc:object-configuration)

## Data versioning

With DBND, you can report and reuse pipeline data inputs and outputs. This is helpful for tracking information about your data sets, pipeline statuses, and your model performance in a machine learning example.

Tracking your data inputs and outputs, with the ability to capture full versions of your data sets, makes it possible to reproduce pipeline runs. If a pipeline fails or you need to change the task code, you can test iterations on consistent data.

To learn more about data versioning details, see [Inputs](doc:inputs) and [Outputs](doc:outputs) ].

## Compute Engine
DBND provides you a way to dynamically switch between local and remote spark, local and k8s execution, [Run](doc:running-pipelines) your driver locally or at [Kubernetes](doc:kubernetes-cluster).


![Introducing DBND](https://files.readme.io/7fc0a4e-Introducing_DBND.png)





## Key concepts

## Tasks

In DBND, a task is a function annotated with a DBND decorator (`@task`). Through the decorator, the task can be tracked and run using the `dbnd run` command in CLI or Python code.

## Pipelines

A pipeline is a sequence of tasks wired together. Machine learning teams usually use pipelines for running data ingestion, aggregations, and model training.

## DAGs

*DAG* is a synonym for *Pipeline* and often used interchangeably. In the DBND documentation, the DAG term is used to denote an Airflow pipeline.

## Runs

A run is an execution of a pipeline or a task.

## Parameters

Parameters are all the defined and changeable properties of a task or pipeline, for example, data input and model weights.

## Environments

Environments define the location where pipelines are executed (for example, local machine, Docker, Kubernetes, Spark cluster) and the metadata store where artifacts are persisted (i.e., input source, system folders, temporary folders, output destination).

##Third-Party Products Used in the Orchestration Mode

###Cloud Environments Types Support
  * GCP
  * AWS
  * Azure

###Environments
 * Local
 * Docker
 * K8s

###File System Support
  * Hadoop Distributed File System (HDFS)
  * Amazon Simple Storage Service (S3)
  * Google Storage (GS)

###Spark Engines Support
* Local Spark
* AWS EMR
* GCP DataProc
* Databricks
* Qubole
* Any Spark server via Apache Livy

### IDEs Support
  * PyCharm
  * Jupiter


## Not Another Orchestration Framework
DBND is not another orchestration library. There are many existing solutions that are improving quite quickly. DBND solves problems for data engineering teams that need to go beyond creating scheduled DAGs - those teams who are looking to adopt agile development methodologies and build a full lifecycle around their projects.

In fact, we encourage you to use DBND along with a production orchestration system like Airflow.