---
"title": "Defaults for Engines and Nested Tasks"
---
In some cases, defining defaults for parameters in the configuration or in the constructor is not sufficient. For example:

* Setting a different default for a config parameter (e.g. main_jar in [spark])
* Setting a different default for all child tasks


### task_config
Represents a dictionary of arbitrary configuration params that will be applied to "current" configuration during task creation and run

<!-- noqa -->
```python
my_task(task_config={SparkConfig.num_of_executors:3})
```


## To set a default value for a task
Use the `defaults` attribute as shown in the following examples:

<!-- noqa -->
```python
class WordCountTask2(WordCountTask):
    defaults = {
        SparkConfig.main_jar: "jar2.jar",
        DataTask.task_env: DataTaskEnv.prod,
        WordCountTask.text: "/etc/hosts",
    }
```

<!-- noqa -->
```python
class AllWordCounts2(AllWordCounts):
    defaults = {
        SparkConfig.main_jar: "jar2.jar",
        DataTask.task_env: DataTaskEnv.prod,
        WordCountTask.text: "/etc/hosts",
    }
```
In the example above, we have set the default values for `WordCountTask` and `WordCountPySparkTask` tasks.