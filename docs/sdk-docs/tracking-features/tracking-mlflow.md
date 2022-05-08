---
"title": "Tracking MLFlow"
---
If you use [MLFlow](https://github.com/mlflow/mlflow), you can duplicate Databand metrics to the MLFlow store and maintain data in the MLFlow system as well.

## To Integrate MLFlow with Databand

1. Run the following command to install the integration plugin:

```bash
pip install databand[mlflow]
```

2. Add the following configuration to your [SDK Configuration](doc:dbnd-sdk-configuration)

```ini

[mlflow_tracking]
# Enable tracking to Databand store
databand_tracking=True

# Optionally, define a URI for mlflow store; mlflow.get_tracking_uri() is used by default
duplicate_tracking_to=http://mlflow-store/
```



## Task Example

The following example code shows how the logging works.

```python
from dbnd import task
from mlflow import start_run, end_run
from mlflow import log_metric, log_param
from random import random, randint

@task
def mlflow_example():
    start_run()
    # params
    log_param("param1", randint(0, 100))
    log_param("param2", randint(0, 100))
    # metrics
    log_metric("foo1", random())
    log_metric("foo2", random())
    end_run()
```

## Execution Flow
When you run `dbnd run mlflow_example`, the following happens in the backend:
1. Databand creates a new DBND context
2. `dbnd_on_pre_init_context` hook from `dbnd_mlflow` is triggered
3. A new URI is computed to be used by mlflow
For example: `dbnd://localhost:8081?duplicate_tracking_to=http%253A%252F%252Fmlflow-store%253A80%252F
4. The new URI is set to be used with `mlflow.set_tracking_uri()`
5. `mlflow_example` task starts:
6. `mlflow.start_run()`
7. `mlflow` reads `entry_points` for each installed package and finds:
  ```
  "dbnd = dbnd_mlflow.tracking_store:get_dbnd_store",
  "dbnd+s = dbnd_mlflow.tracking_store:get_dbnd_store",
  "databand = dbnd_mlflow.tracking_store:get_dbnd_store",
  "databand+s = dbnd_mlflow.tracking_store:get_dbnd_store",
  ```
8. `mlflow` creates `TrackingStoreClient` by using the new URI
9. URI schema instructs to use `dbnd_mlflow.tracking_store:get_dbnd_store`
    * `get_dbnd_store` creates dbnd `TrackingAPIClient`
    * `get_dbnd_store` creates mlflow tracking store to duplicate tracking to
    * `get_dbnd_store` returns `DatabandStore` instance
10. `log_param()`/`log_metric()`
    * calls to `DatabandStore`
    * calls to `TrackingAPIClient`
    * calls to mlflow tracking store to duplicate tracking to `mlflow.end_run()`
12. `mlflow_example` ends
13. `dbnd_on_exit_context` hook from `dbnd_mlflow` is triggered
14. Restore the original mlflow tracking URI.


[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>"
}
[/block]