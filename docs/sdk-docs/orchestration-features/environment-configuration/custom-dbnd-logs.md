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