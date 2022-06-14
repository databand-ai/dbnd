---
"title": "Task Meta"
---
## Task identification `TaskPassport`

 Task identifier has 3 components:
* Task Namespace
* Task Family
* Task ID

While Task Namespace and Task Family are the code's identifiers, the ID is the instance identifier.
To override the default Namespace, you need to set `Task.task_namespace` property. Look at the `PrepareData` task in the example below:

<!-- noqa -->
```python
prepared_data = PrepareData(data='just some data')
print(prepared_data)                 # --> "prepare_data__19d1172be0"

print(prepared_data.task_namespace)  # --> ""
print(prepared_data.task_family)     # --> "prepare_data"
print(prepared_data.task_id)         # --> "prepare_data__19d1172be0"

print(PrepareData.task_namespace)   # --> ""
print(PrepareData.task_family)      # --> "<property object at 0x1111e9548>"
print(PrepareData.task_id)          # --> Error!
```

### Q: How to get access the current task's object?
A: You can access your current task (during pipeline build or task execution) via `current_task`, and the current run - via `current_task_run`.

```python
from dbnd import current_task, task

@task
def calculate_alpha(alpha: int = 0.5):
    return current_task().task_version
```

### Q: Is there a way to access the current env inside a task or pipeline?
A: Yes, you can access it via your current task.

```python
from dbnd import current_task, task

@task
def calculate_alpha(alpha: int = 0.5):
    return current_task().task_env.name  # The environment of the task
                                  # See EnvConfig object for all properties
```

### Q: How can I find the user running the pipeline in the `task_run`?
A: Using the `task_run_env` property of `task_run` that holds the user's context of the run, including with the user name.

<!-- noqa -->
```python
task_run.task_run_env.user
```
See more about `task_run_env` [here](doc:tasks-pipelines-data#run-info).

### How to access all user facing task parameters programmatically?

<!-- noqa -->
```python
from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd import task

all_params = {param.name: param.value for param in task.task_params.get_param_values(ParameterFilters.USER)}
```
### How to access all input task parameters?

<!-- noqa -->
```python
from dbnd._core.parameter.parameter_value import ParameterFilters

all_params = {param.name: param.value for param in task.task_params.get_param_values(ParameterFilters.INPUTS)}
```

See [Run Info](doc:run-info) for accessing run environment info.