---
"title": "Task Discovery"
---
## Referring to a specific task using its fully qualified name
The simplest way to find a task is mentioning the task by its fully qualified name, or `FQN`. A fully qualified name is made of the following components: `m`.`m1`.`m2`.`function`
Or, in the CLI perspective:
`dbnd run m.m1.m2.function`
(there can be any number of packages on the way to a certain function).

Fully qualified names are useful because they allow us to refer to a specific task in a global way, independent from the working directory or environment.


## Referring to a specific task using the task registry
Whenever modules that contain decorated tasks (`@task` or `@pipeline`) are loaded, the tasks are registered into the `TaskRegistry`. The advantage of referring to tasks via the registry is the ability to call them by their "task family name", which is simply the function's name. 

If you look at the the example in the section above, we could omit the `package` and `module` prefixes, and only use the `function` name, as in: `dbnd run function`.

There are several ways to load tasks into the task registry ahead of time:
1. Referring to modules that require import with the `-m` CLI flag. For example:
`dbnd run -m module_to_load task_to_run`
In this scenario, we will be loading the module named `module_to_load` and running the task named `task_to_run`.
2. Loading modules via DBND's plugin system.
3. The module parameter in the `databand` configuration section includes a list of python modules provided out-of-the-box that are imported automatically. 

```ini
[databand]
module = databand,databand_examples,my_module 
```