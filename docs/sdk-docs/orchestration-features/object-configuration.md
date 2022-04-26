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

DBND loads configuration from the custom files after reading the configuration specified in the system configuration files. For information about the sequence of reading the configuration from the config files, see [SDK Configuration](doc:setting-up-configuration-with-files)].

## Can I change configuration via Environment Variables?
You can set any system parameter by using environment variables.
For example, you can override the `databand_url` parameter under the `core` section of the **project.cf** file by setting a value for the  `DBND__CORE__DATABAND_URL` environment variables.
Similarly, you can construct environment variables for other configuration parameters by adding `DBND__<SECTION_NAME>__`  prefix to the parameter name.


## Q: How do I supply parameters programmatically \ dynamically?
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

## Q: How do I inject `None` as a parameter value from CLI/config
You should use the alias `@None`

## Q: How do I inject path as a target to the task from CLI/config
You should use the alias `@target:`.  Use it like this: `--set @target:/path/to/file.txt`

## Q: How do I inject a value that starts with `@`
Please use `@@` instead. For example, if you need to use `@my_value@`, you should use `--set @@my_value@`

## Q: How do I pass a list parameter in CLI?
A: The syntax is as follows ` --set list_param="[1, 2, 3, 'str1']"`

## Q: How do I pass a dictionary parameter in CLI?
A: The syntax is as follows `--set dict_param="{'key1': 'value1', 'key2': 'value2', 255: 3}"`
or `--set dict_param="key1=value1, key2=value2"`

## Q: Can  I change the parameter of any class?
A: Yes!  All parameters are also exposed on a class level on the command line interface. For instance, say you have class inner task "prepare_data", you can use. "--set prepare_data.ratio=3"

## Q: Can I provide values via json?
A: Yes, you can use  `--set "{\"TaskA.x\":\"some value\", \"TaskB.y\":\"another\"}"`
