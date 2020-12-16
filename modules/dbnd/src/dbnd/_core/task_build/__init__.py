"""
Task build process
=================

When a task is created:
 * We calculate task_env and task_config first
 * for every property, we look at all possible sections ( full task name, task name, and inherited tasks)
 * Config Layers:
     system layer ( starting point, contains Configuration, Environment, and Command-line
     new layer from task.task_definition.defaults (Task.defaults)  merge_strategy=on_non_exist_only
     updated with overrides from constructor
     updated with task_config
     updated with task_env values

Special properties of the Task
    Task.defaults is the lowest level of config (DEFAULTS)
     * definition: custom property of the Task object
     * definition: all values are merged when TaskB inherits from TaskA (at TaskDefinition)
     * task.defaults value is calculated at TaskDefinition during the "compilation of the Task class definition"
     * usage: its value is merged into Current config (only updates nonexisting fields) at TaskMetaClass
     * do not support override()!!!
     * there is no "merge" behavior for values. (Dictionaries are going to be replaced)
         class TaskA(...):
            defaults = {SparkConf.conf : { 1:1} , SparkConf.jar : "taskajar }

         class TaskB(TaskA):
            defaults = {SparkConf.conf : { 1:2}  }
        TaskB.defaults == {SparkConf.conf : { 1:2} , SparkConf.jar : "taskajar }

    Task.task_config - overrides current config level
     * definition: regular parameter of the task
     * definition: it "replaces" inherited task_config from super class if exists
     * its value is calculated every time we create Task at TaskMeta
     * usage: it updates current config layer (
     * supports override():  `task_config = {SparkConf.jar: override("my_jar")}`
     * Usually, it's used with some basic "nested" config param: like SparkConfig.conf.
     Task.task_config issues:
       1.     task_config = {SparkConf.jar: "my_jar"}
             [spark_remote]
             jar=from_config
        User will get my_jar when engine is [spark] and  "from_config" if engine is [spark_remote].
        The reason is that engine looks for the value in most relevant section which is [spark_remote],
        while SparkConf.jar provides a value for [spark] section
        The only way to workaround it right now is to use:
            task_config = {SparkConf.jar: override("my_jar")}
        The system will look for override values and if it exists,
        the override will be taken (despite best section match).

    Task(overrides=conf_value...)
     * uses the same system as task_config, but override is applied to all values

"""
