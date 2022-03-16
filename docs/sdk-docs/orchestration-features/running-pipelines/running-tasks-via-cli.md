---
"title": "Run a Task via CLI"
---
To run any command in DBND, use the following command pattern:
 `dbnd COMMAND [ARGS]`

To list the available DBND commands, run `dbnd --help`.
To display arguments for any DBND command, use the `--help` argument.

## Orchestration CLI Reference  

| Command | Description |
|---|---|
| run | Run a task or a pipeline |
| project-init | Initialize the project structure |
| show-configs | Show and search configurations. |
| show-tasks | Show and search a tasks. |
| ipython | Get iPython shell with Databand's context |

## `dbnd run` Command Usage

### `dbnd_run` Command Options

| Command | Description | Example |
|---|---|---|
| -s, --set | Sets a configuration value | --set task_name.task_parameter=value |
| --set-config | Sets a configuration value (key=value) |  |
| -r, --set-root | Sets a main task parameter value without specifying task name. Allows to override specific parameters. | `dbnd run predict_wine_quality --set-root alpha=5`   In this example, **alpha** is overridden. |
| -o, --override | Overrides a configuration value (key=value). Has higher priority than any config source. |  |
| --conf | Defines a list of files to read from. |  |
| -m, --module | [Loads modules dynamically](doc:unpublished-cli-commands-reference). Allows to add a path of a module where DBND can search for tasks/pipelines. | `dbnd run dbnd_sanity_check --module /path/to/module.py` |
| -v, --verbose | Makes the logging output more verbose. |  |
| --describe | Describes the current run. |  |
| --env | Task environment: local/aws/aws_prod/gcp/prod  [default: local] |  |
| --parallel | Runs specific tasks in parallel. |  |
| --task-version | Sets a task version; it directly affects the task signature. |  |
| --project-name | Name of this Databand project. |  |
| --description | Sets description of the run. | dbnd run dbnd_sanity_check --description some_description |
| --task-name | Sets name of the task. | dbnd run dbnd_sanity_check --task-name my_name |
| --override-run-uid | Use predefined uid for run. | dbnd run dbnd_sanity_check --override-run-uid 00000000-0000-0000-0000-000000000000 |
| --help | Is used for dynamic loading of modules |  |

##Running only specific tasks (according to regex):
You can use  `--set run.selected_tasks_regex=task_name_regex`
You can also give it a list of regular expressions, and it will run all the tasks which conforms with those regular expressions: `--set run.selected_tasks_regex="['regex1', 'regex2', ..]"`
**Note: running tasks like this will also run all of the tasks your diluted run depends on**