---
"title": "Running Pipelines"
---
DBND provides a command-line interface that enables you to run pipelines and tasks, and configure the installation.

The basic syntax is `dbnd <command name>` followed by positional arguments and then one or more named arguments.

```bash
$dbnd run --help
```

## Run Command

The `run` command is used to run DBND tasks and pipelines from the command line. Run specific and global configuration parameters can also be set from the command line.

```python
from dbnd import task

@task
def prepare_data(data: str) -> str:
    return data
```

```bash
$dbnd run prepare_data
```

See [Task Discovery](doc:task-discovery)  for the proper way of referencing tasks in the command line.


## Running tasks in parallel
It is possible to run tasks in a pipeline in parallel with the help of `local_parallel` executor. You can use `--parallel`, e.g. `dbnd run <task_name> --parallel`.  Keep it in mind that `--parallel` is only supported with dbnd-airflow installed. If you still get errors when trying to run tasks in parallel, make sure that Airflow is specified in the cfg file. Make sure to use a non-SQLite database for your Airflow or you won't be able to run tasks in parallel.


Example:
```bash
dbnd run calculate_alpha --task-version now --parallel
```


> Use `--open-web-tab`  to open the result of a run in a new tab in the Databand UI.