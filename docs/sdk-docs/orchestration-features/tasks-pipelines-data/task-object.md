---
"title": "Task"
---
## Task Signature
Every task is unique. It has a name, parameters, inputs, and outputs.

Based on the task's main properties, task signature is calculated. When you run a task and one of the inputs changes, DBND automatically changes the signature, and the pipeline restarts (runs from the beginning). Every time you change a significant input parameter in a task, it will run again. If one of the task inputs from another task has been changed, this task will also run again.

### Task Version
Every task has a parameter called `task_version`. This parameter represents the current "version" of the task. If the code/external inputs of one of the tasks in the pipeline changed in a way it didn't affect the signature of the task, or you want to rerun a specific task for some reason, you need to change the `task_version` parameter. Task version is a part of the signature so the task will be re-executed.

<!-- noqa -->
```python
prepare_data(task_version="1")
# Use any string value to create a version of the task.
prepare_data(task_version="now")
```
This can also be done through the console:
```bash
$ dbnd run prepare_data --task-version now
```

> There are self-resolved aliases that can be used together with `task_version`:
>   `now` - will be resolved into the current time
>  `GIT`   - will be resolved into project git version

You can also set specific task version, to a task inside a pipeline, for example:
task named "task_name" to version "now" by using the `--set` flag like this:
```bash
$ dbnd run pipeline_name --set task_name.task_version=now
```

### Task Class Version
If you want to have a persistent change in your task signature, you can use `task_class_version` (default="1"). It will affect the output paths of the task.
```python
from dbnd import task
from pandas import DataFrame

@task(task_class_version=2)
def prepare_data(data: DataFrame) -> DataFrame:
    return data
```

### Task Target Date
Another important property of a  task is `task_target_date` - it is a date associated with the task execution. This property represents the "logical" date when the data is created by your task.

In the code:

<!-- noqa -->
```python
import datetime
prepare_data.dbnd_run(task_target_date=datetime.date.today())
```

Console syntax:
```bash
$ dbnd run prepare_data --set target_date=2019-12-31
```

## Task Uniqueness

In order to cut down on unnecessary computing time, DBND does not run non-unique tasks, leveraging the pre-calculated results.

By default, tasks are uniquely identified by their class name and the values of their parameters. Within the same worker, two tasks of the same class with parameters having the same values are not just equal but are the same instance. You can configure this behavior as desired.

<!-- noqa -->
```python

c = prepare_data(alpha=0.5)
d = prepare_data(alpha=0.5)

assert c is d
```

If you have more than one instance of the same task class in your code, you need to configure each task class using a different `task_name`. This way, you will be able to configure each of such task instances separately.

Let’s consider an example:

<!-- noqa -->
```python
from dbnd import pipeline

@pipeline
def prepare_data_pipeline():
    prepare_data(task_name="first_prepare_data")
    prepare_data(task_name="second_prepare_data")
```

Here two different task names are in use, so you can configure the data of the first `prepare_data` to ‘special_data’ and the second  `prepare_data` would have the default value using the following command:
```bash
dbnd run my_module.PrepareDataPipeline --task-version now --set first_prepare_data.data=special_data
```

This will configure the data of the first  `prepare_data` to `special_data`, while the second  `prepare_data` will keep the default value.

## Running Task In Task

Dynamic runs will run the task as a function call and will not persist the task's intermediate results. In the following example, a `task` and a `pipeline` are running in memory as functions:

<!-- noqa -->
```python
import logging
from dbnd import task

dbnd_run_start()

@task
def calculate_alpha(alpha: int):
    calculated_alpha = "alpha: {}".format(alpha)
    logging.info(calculated_alpha)
    return calculated_alpha

@task
def prepare_data(data, additional_data):
    return "{} {}".format(data, additional_data)

@task
def prepare_dataset(data, data_num: int = 3) -> str:
    result = ""
    for i in range(data_num):
        result = prepare_data(result, data)

    return result
```

`Task.task_is_dynamic` indicates that the task is dynamic - executed from within another task as a single unit of work.

`Task.task_supports_dynamic_tasks` property indicates that the task can run dynamic DBND tasks.

## Advanced Task Properties

###task_enabled
|**Data Type**| **Default** | **Description**|
|---|---|------------------|
|Bool | True | Enable/disables task execution. |


###task_enabled_in_prod
| Data Type | Default Value | Description|
|-----------|---------------|----------------------------------------------------------------------------|
| Bool      | True          | When `false`, the task will not run when the environment is set to `prod`. |


###task_is_system
| Data Type | Default Value | Description|
|---|---|---|
| Bool | False | This property indicates that the task is a system task. |

###task_band
| Property | Description |
|---|---|
| Target | Location of a task band output on the disk. It contains information about the names and physical location of the task outputs. It is managed by the system and you should not change it in most cases. |

###task_env
| Data Type | Default | Description |
|---|---|---|
| Str | Local | Task environment defines data and compute environment for a task. <br> This property is a part of a task signature. |
