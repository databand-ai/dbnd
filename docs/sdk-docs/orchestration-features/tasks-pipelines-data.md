---
"title": "Tasks, Pipelines, Data"
---
## Tasks

In DBND, a task is a modular unit of computation, usually a data transformation with an input and an output. 
  
To create a task, you need to decorate your function with the `@task` decorator. Under the hood, the `task` object is created, and the function parameters are mapped to the task parameters, and the function returns value to task outputs.
           
```python
@task
def prepare_data(data: pd.DataFrame) -> pd.DataFrame:
    return data
```

## Pipelines

A pipeline is a collection of tasks wired together to run in a specific order. Pipelines define tasks and data flow. Pipelines encapsulate an execution plan that can run as a typical python function.

```python
@task
def train_model(data: pd.DataFrame) -> object:
    ...

@pipeline
def prepare_data_pipeline(data: pd.DataFrame):
    prepared_data =  prepare_data(data)
    model = train_model(prepared_data)
    return model
```

The DBND runtime consists of two stages: 
* Dependency resolution 
* Task execution. 

`Pipeline.band` (or pipeline function) will run at the first stage - dependency resolution at the very beginning of your run. `Task.run` will be called in the second phase. 

When DBND "reads" pipelines - during the dependency resolution time and before the execution time, it substitutes the future output for `targets`. `Targets` signal that DBND should resolve this parameter during execution.

## Data
DBND task controls how inputs and outputs are loaded and saved. 

```python
@task
def prepare_data(data: DataFrame) -> DataFrame:
    data["new_column"] = 5
    return data
```

By default, DBND detects input and output data types based on [Python typing](https://pypi.org/project/typing/). This enables a lot of out of the box functionality, such as loading dataframes from different file formats and automatically serializing the outputs.

Let's consider an example that demonstrates how tasks or pipelines can be run on "development" and "production" data.
 
```bash
$ dbnd run example.prepare_data --set data=myfile.csv

$ dbnd run example.prepare_data --set data=s3://my_backet/myfile.json
```

The first command here will:
* Load data from a local CSV file
* Inject it into the `data` parameter
* Run the `prepare_data` task 

The DataFrame returned by this task will be serialized into a CSV file according to the system's default behavior.

The second command will:
* Run the same task, but this time loading data from a different JSON file located in AWS S3. 

> 📘 **How to change parameter value?**
>
> see [DBND Object Configuration](doc:object-configuration)