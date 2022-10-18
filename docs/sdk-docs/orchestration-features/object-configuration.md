---
"title": "Task Configuration"
---
In DBND, you can configure parameters and their corresponding values for the following objects:
  * Task
  * Pipelines
  * DBND Configuration objects: Environments, Engines, and others.

You can configure these objects by using one of the following methods:
  * Running commands in CLI
  * Changing python code for tasks
  * Setting up configuration in the configuration files

For example, you have the following task definition:

```python
from dbnd import task
from pandas import DataFrame

@task
def prepare_data(data: DataFrame, key: str) -> str:
    result = data.get(key)
    return result
```


You can define new default values for `data` or `key` in a configuration file using `task_family` and the name of the property:

```buildoutcfg
[prepare_data]
data=<path to a file>
key="alpha"
```
You can also change those params on the CLI:   `--set prepare_data.key="alpha"`

For example, you have the Kubernetes engine settings defined inside a configuration file:
```buildoutcfg
[kubernetes]
_type = dbnd_docker.kubernetes.kubernetes_engine_config.KubernetesEngineConfig
submit_termination_grace_period = 30s
keep_finished_pods=False
```

You can change Kubernetes settings by running the following command in CLI:
```bash
dbnd run dbnd_sanity_check --env k8s --set kubernetes.keep_finished_pods=True
```
You can also override values of sub pipelines as described [here](doc:configuration-layers).

## Before you begin

Review [Configuration Layers](doc:configuration-layers) to understand what configuration layers are supported in DBND and how they work together. You must understand what configuration layers are supported in DBND as the information specified at a top configuration layer overrides configuration at a lower layer. For more details about these layers, see [Configuration Layers](doc:configuration-layers).


## Setting Parameters
`dbnd` allows you to set pipeline and task as well as other configuration values through CLI with the following interface:
`--set KEY=VALUE` will set the value of KEY to VALUE.

You can run a pipeline and set its parameter values from CLI:
```bash
$dbnd run prepare_data_pipeline --set data=Data
```

Injecting parameter values for a task inside a pipeline. i.e. setting a value for the `data` parameter of `prepare_data` task:
```bash
$dbnd run prepare_data_pipeline --set prepare_data.data=Data
```

`prepare_data_pipeline` gets string parameters. To load `data` from a file use `@` before the value. This indicates the value is actually a file path:

```bash
$dbnd run prepare_data_pipeline --set data=@data_file.txt
```

For complex types, for example -   `pandas.DataFrames` and `NumPy` arrays, strings are handled as paths by default.


> **Remember**
>
> * Settings defined inside configuration files can be replaced at three different points of execution.
> * CLI commands always override task definitions specified in DBND configuration files.

## What is the simplest configuration method to start with?
We recommend setting up the configuration for tasks and environments by using configuration files. See [Configuration files overview](doc:dbnd-sdk-configuration).


## Loading Configuration from Custom Files
You can also write configuration in any file.  In order for the DBND to read this configuration, you need to run tasks by using the `--conf-file` parameter.
In the following example command, DBND reads configuration specified in your custom file:
```shell
dbnd run prepare_data_pipeline --task-version now --conf-file=/users/myuser/development/fdataband/myconfig.cfg
```
As it is shown in this example, for the `conf-file` parameter you specify the path to the file where the custom configuration file is saved.

DBND loads configuration from the custom files after reading the configuration specified in the system configuration files. For information about the sequence of reading the configuration from the config files, see [SDK Configuration](doc:dbnd-sdk-configuration)].

## `[run]` Configuration Section Parameter Reference
- `name` - Specify the run's name.
- `description` - Specify the run's description
- `parallel` - Run specific tasks in parallel.
- `task_executor_type` - Set alternate executor type. Some of the options are `local`, `airflow_inprocess`, `airflow_multiprocess_local`, or `airflow_kubernetes`.
- `enable_airflow_kubernetes` - Enable the use of Kubernetes executor for kubernetes engine submission.
- `interactive` - When submitting a driver to remote execution, keep track of the submitted process and wait for completion.
- `submit_driver` - Override `env.submit_driver` for the specific environment.
- `submit_tasks` - Override `env.submit_tasks` for the specific environment.
- `open_web_tracker_in_browser` - If True, opens web tracker in browser during the task's run.
- `is_archived` - Determine whether to save this run in the archive.
- `dry` - Do not execute tasks, stop before sending them to the execution, and print their status.
- `run_result_json_path` - Set the path to save the task band of the run.
- `debug_pydevd_pycharm_port` - Enable debugging with `pydevd_pycharm` by setting this to the port value expecting the debugger to connect. This will start a new `settrace` connecting to `localhost` on the requested port, right before starting the driver task_run.
- `execution_date` - Override the run's execution date.
- `mark_success` - Mark jobs as succeeded without running them.
- `id` - Set the list of task IDs to run.
- `selected_tasks_regex` - Run only specified tasks. This is a regular expression.
- `ignore_dependencies` - The regex to filter specific task_ids.
- `ignore_first_depends_on_past` - The regex to filter specific task_ids.
- `skip_completed` - Mark jobs as succeeded without running them.
- `fail_fast` - Skip all remaining tasks if a task has failed.
- `enable_prod` - Enable production tasks.
- `skip_completed_on_run` - Determine whether dbnd task should check that the task is completed and mark it as re-used on task execution.
- `validate_task_inputs` - Determine whether dbnd task should check that all input files exist.
- `validate_task_outputs` - Determine whether dbnd task should check that all outputs exist after the task has been executed.
- `validate_task_outputs_on_build` - Determine whether dbnd task should check that there are no incomplete outputs before the task executes.
- `pipeline_band_only_check` - When checking if the pipeline is completed, check only if the band file exists, and skip the tasks.
- `recheck_circle_dependencies` - Recheck circle dependencies on every task creation, use it if you need to find a circle in your graph.
- `task_complete_parallelism_level` - Set the number of threads to use when checking if tasks are already complete.
- `pool` - Determine which resource pool will be used.
- `always_save_pipeline` - Enable always saving pipeline to pickle.
- `disable_save_pipeline` - Disable pipeline pickling.
- `donot_pickle` - Do not attempt to pickle the DAG object to send over to the workers. Instead, tell the workers to run their version of the code.
- `pickle_handler` - Define a python pickle handler to be used to pickle the run's data
- `enable_concurent_sqlite` - Enable concurrent execution with sqlite database. This should only be used for debugging purposes.
- `heartbeat_interval_s` - Set how often a run should send a heartbeat to the server. Setting this to -1 will disable it altogether.
- `heartbeat_timeout_s` - Set how old a run's last heartbeat can be before we consider it to have failed.Setting this to -1 will disable this.
- `heartbeat_sender_log_to_file` - Enable creating a separate log file for the heartbeat sender and don't log the run process stdout.
- `hearbeat_disable_plugins` - Disable dbnd plugins at heartbeat sub-process.
- `task_run_at_execution_time_enabled` - Allow tasks calls during another task's execution.
- `task_run_at_execution_time_in_memory_outputs` - Enable storing outputs for an inline task at execution time in memory. (do not use FileSystem)
- `target_cache_on_access` - Enable caching target values in memory during execution.


## FAQ

### Q: Can I change configuration via Environment Variables?
A: You can set any system parameter by using environment variables.
For example, you can override the `databand_url` parameter under the `core` section of the **project.cf** file by setting a value for the  `DBND__CORE__DATABAND_URL` environment variables.
Similarly, you can construct environment variables for other configuration parameters by adding `DBND__<SECTION_NAME>__`  prefix to the parameter name.


### Q: How do I supply parameters programmatically \ dynamically?
A: Supplying parameters dynamically is possible via python functions. You can provide parameters by calling a function that can determine how your pipeline will be run.

For example, let's say we want our run's name to be determined by the current timestamp, but we don't want to input it manually every time.

To achieve this, we can use the python function notation for parameters:
`dbnd run my_task --set run.name="@python://my_package.my_module.my_func"`
And on python side, inside `my_package`, inside `my_module`:
```python
import time
def calculate_alpha():
    return "alpha=%s" % (time.time(), )
```
The python function can access configuration and other external resources.

### Q: How do I inject `None` as a parameter value from CLI/config
You should use the alias `@None`

### Q: How do I inject path as a target to the task from CLI/config
You should use the alias `@target:`.  Use it like this: `--set @target:/path/to/file.txt`

### Q: How do I inject a value that starts with `@`
Please use `@@` instead. For example, if you need to use `@my_value@`, you should use `--set @@my_value@`

### Q: How do I pass a list parameter in CLI?
A: The syntax is as follows ` --set list_param="[1, 2, 3, 'str1']"`

### Q: How do I pass a dictionary parameter in CLI?
A: The syntax is as follows `--set dict_param="{'key1': 'value1', 'key2': 'value2', 255: 3}"`
or `--set dict_param="key1=value1, key2=value2"`

### Q: Can  I change the parameter of any class?
A: Yes!  All parameters are also exposed on a class level on the command line interface. For instance, say you have class inner task "prepare_data", you can use. "--set prepare_data.ratio=3"

### Q: Can I provide values via json?
A: Yes, you can use  `--set "{\"TaskA.x\":\"some value\", \"TaskB.y\":\"another\"}"`
