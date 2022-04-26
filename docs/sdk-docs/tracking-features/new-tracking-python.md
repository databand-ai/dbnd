---
"title": "[New] Tracking Python"
---
If you are running python, Databand can provide visibility into your data operations, code errors, metrics, and logging information, in the context of your broader pipelines or orchestration system.

These are the available tracking options for Python:

* [Metrics](doc:metrics)
* [Datasets](doc:tracking-python-datasets)
* [Histograms](doc:histogram)

# Tracking Context
To enable tracking, call your code within the `dbnd_tracking()` context.
```python
from dbnd import dbnd_tracking
if __name__ == "__main__":
   with dbnd_tracking():
      pass
```

Any Python code executed inside the `dbnd_tracking()` context will be tracked by Databand.

Alternatively, you can call `dbnd_tracking_start()` at the beginning of the script.
```python
from dbnd import dbnd_tracking_start
if __name__ == "__main__":
   dbnd_tracking_start()
   pass
```

Any Python code executed after the `dbnd_tracking_start()` will be tracked by Databand.

You can pass the following parameters to both `dbnd_tracking()` and `dbnd_tracking_start()`
  * `job_name` - Name of the pipeline (in the Pipelines screen)
  * `run_name` - Name of the run
  * `project_name`  - Name of the project of the pipeline
  * `conf` - Dictionary with Databand configuration

In order to be able to connect to databand UI, you will need to provide the URL and the access token (see [Connecting DBND to Databand (Access Tokens)](doc:access-token))
This can be done using the conf parameter:
```python
from dbnd import dbnd_tracking_start

conf_dict = {
    "core": {
        "databand_url": "<databand_url>",
        "databand_access_token": "<access_token>",
    },
    "tracking": {
        "track_source_code": False
    }
}
if __name__ == "__main__":
   dbnd_tracking_start(conf=conf_dict)
   pass
```
You can also choose whether Databand will track your source code using the `track_source_code` parameter (set to False by default).
You can read more in [SDK Configuration](doc:dbnd-sdk-configuration) on how to add parameters to the tracking context

If you are using [Tracking Airflow DAGs](doc:tracking-airflow-dags) you don't need to enable tracking for python code executed as part of Airflow Operator. This is done automatically.


## Tracking Functions with Decorators
For a better visibility, you can also annotate your function with a decorator.
Below is an example in a Python function, though decorators for [Java and Scala](doc:JVM) functions are supported as well.

```python
from dbnd import task
import pandas as pd

# define a function with a decorator

@task
def user_function(pandas_df: pd.DataFrame, counter: int, random: int):
    return "OK"
```

For certain objects passed to your functions such as Pandas DataFrames and Spark DataFrames, DBND automatically collects data set previews and schema info. This makes it easier to track data lineage and report on data quality issues.

You can implicitly enable tracking, so the first @task will start tracking your script by having the environment variable `DBND__TRACKING` set to `True`. This will enable tracking with or without dbnd_tracking() context applied.

``` bash
export DBND__TRACKING=True
```

### Tracking Specific Functions without changing module code

Let us say we would like to track a function (or functions) from a module. Instead of decorating each function with `@task`, you can use the `track_functions` function.

Review the following example, where `module1` contains `f1` and  `f2` functions:
<!-- xfail -->
```python
from module1 import f1,f2

from dbnd import track_functions
track_functions(f1, f2)
```

The `track_functions` function uses functions as arguments and automatically decorates them so that you can track any function without changing your existing function code or manually adding decorators.

### Tracking Modules

For an easier and faster approach, you can use the `track_module_functions` function to track all functions inside a named module. So, `module2.py` from the above example would look like this:
<!-- xfail -->
```python
import module1
from dbnd import track_module_functions

track_module_functions(module1)
```

To track all functions from multiple modules, there is also `track_modules` which gets modules as arguments and tracks all functions contained within those modules:
<!-- xfail -->
```python
from dbnd import track_modules

import module1
import module2

track_modules(module1, module2)
```
