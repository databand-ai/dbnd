---
"title": "Logging System"
---
DBND creates default logging behavior on initialization. By default, DBND saves separate logs for every run, on the pipeline and task level. You can control the creation, format, and level of logs.

When Databand SDK runs as part of Airflow UI we will use [Airflow Logging](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/logging-tasks.html) subsystem.

The default logging configuration includes the following log definitions:
```
[log]
disable = False
# Logging level
level = INFO
stream_stdout=False

# Logging format
formatter = [%%(asctime)s] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s
formatter_simple = %%(asctime)s %%(levelname)s - %%(message)s
formatter_colorlog = [%%(asctime)s] %%(log_color)s%%(levelname)s %%(reset)s %%(task)-15s - %%(message)s

console_formatter = formatter_colorlog
file_formatter = formatter

sentry_url =

loggers_at_warning = azure.storage,flask_appbuilder
```

You can overwrite the default behavior in the DBND [configuration files](doc:dbnd-sdk-configuration).
To write logs to stdout for Jupyter or PyCharm, apply the following configuration:
```ini
stream_stdout=True
```

You can always disable DBND logging by [setting](doc:dbnd-sdk-configuration) `log.disabled` to `True`.
## Log to Sentry

Databand can be configured to send errors to Sentry.

First, install sentry sdk - `pip install sentry_sdk`
Second, configure databand:

``` TOML
[log]
sentry_url = <your sentry dsn>
sentry_env = <errors will be reported to this environment - default "dev">
sentry_debug = <optional flag - enable sentry debug mode>
```

## Log Preview

Databand can save a preview of tasks and the main process logs at [our service](doc:monitoring). `preview_head_bytes` and `preview_tail_bytes` define how much data will be sent to the service (max number of bytes).  The default is 15KB.

```
[log]
preview_head_bytes = 15360
preview_tail_bytes = 15360
```

## `[log]` Configuration Section Parameter Reference
- `disabled` - Determine whether logging should be disabled.
- `debug_log_config` - Enable debugging our logging configuration system.
- `capture_stdout_stderr` - Set if logger should retransmit all output written to stdout or stderr
- `capture_task_run_log` - Enable capturing task output into log.
- `override_airflow_logging_on_task_run` - Enable replacing airflow logger with databand logger.
- `support_jupyter` - Support logging output to Jupiter UI.
- `level` - Set which logging level will be used. This could be DEBUG, INFO, WARN, or ERROR
- `formatter` - Set the log formatting string, using the logging library convention.
- `formatter_colorlog` - Set the log formatting string, using the logging library convention.
- `formatter_simple` - Set the log formatting string, using the logging library convention.
- `console_formatter_name` - Set the name of the formatter logging to console output.
- `file_formatter_name` - Set the name of the formatter logging to file output.
- `sentry_url` - Set the URL for setting up sentry logger. Make sure the url is exposed to dbnd run environment
- `sentry_env` - Set the environment for sentry logger.
- `sentry_release` - Release for sentry logger
- `sentry_debug` - Enable debug flag for sentry
- `file_log` - Determine whether logger should log to a file. This is off by default
- `stream_stdout` - Should Databand's logger stream stdout instead of stderr.
- `custom_dict_config` - Set customized logging configuration.
- `at_warn` - Set name of loggers to put in WARNING mode.
- `at_debug` - Set name of loggers to put in DEBUG mode.
- `exception_no_color` - Disable using colors in exception handling.
- `exception_simple` - Use simple mode of exception handling
- `send_body_to_server` - Enable or disable sending log file to server.
- `preview_head_bytes` - Determine the maximum head size of the log file, bytes to be sent to server. The default value is 0 Kilobytes.
- `preview_tail_bytes` - Determine the maximum tail size of the log file, bytes to be sent to server. The default value is 0 Kilobytes
- `remote_logging_disabled` - For tasks using a cloud environment, don't copy the task log to cloud storage.
- `targets_log_level` - Should log the time it takes for marshalling and unmarshalling targets.
- `disable_colors` - Disable any colored logs.
- `sqlalchemy_print` - Enable sqlalchemy logger.
- `sqlalchemy_trace` - Enable tracing sqlalchemy queries.

