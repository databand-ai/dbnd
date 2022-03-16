---
"title": "Testing & Debugging"
---
To make sure your functions work correctly, you need to implement unit tests. 
We suggest that you test your DBND project by using the Pytest framework.

## Quickstart
To make Databand bootstrap with the right test configuration, database settings, fixtures, etc., configure the `conftest.py` as follows:
```python
import pytest

from dbnd.testing.test_config_setter import add_test_configuration


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    add_test_configuration(__file__)
```

## To Create Unit Tests
Testing DBND is a straightforward process:


* You can use `dbnd_run()` to run your tasks with the requested variables, for example:
```python
alpha = calculate_alpha.dbnd_run(task_env="local", task_version="now")
```

* You can also use `dbnd_run_cmd()` to run `dbnd` with cmd parameters, for example:
```python
alpha = dbnd_run_cmd(["calculate_alpha", "-r alpha=0.4"])
```

## To Handle Exceptions
To make sure that an exception has been thrown from your Databand process, you can use `run_locally__raises`.

```python
def test_cli_raises_ex(self):
    run_locally__raises(
        DatabandRunError,
        [
            "task_raises_exception"
        ]
    )
```

## Databand Test Plugins 
### Setting Up Root for Testing

To set up a temporary Root and make sure that every test runs in its own folder (no data caching):
```python
import pytest
from dbnd.testing.test_config_setter import add_test_configuration

pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin", 
]
```

### Autoload Test Configuration
In order to load your test configuration for Databand automatically before every test, edit your `conftest.py`:
```python   
def pytest_configure(config):
    add_test_configuration(__file__)
```

After this change, when Databand is loaded in a test mode, it will look for a configuration file named `databand-test.cfg` located in the same folder as `contest.py`. `__file__` will be used to identify the location of `databand.test.cfg, the function will assume that it's the same folder.

```
└──── dbnd_tests
    ├── conftest.py
    ├── databand.test.cfg
    └── tests
        ├── unit-tests_1.py
        └── unit-tests_2.py
```
### Test Configuration
This configuration file should contain your specific settings for the tests, such as shown in the following example:

```inicfg
[core]
tracker = ['console']

[run]
task_executor_type = local

[local]
spark_engine = spark_local
root = ${DBND_HOME}/data
dbnd_local_root = ${DBND_HOME}/data/dbnd
```

In this example, you set `tracker = ['console']` in the `[core]` section. This will enable only "console tracking" (aka "print all information to console"), and no API or DB tracking.
It is very useful if you want to check your environment offline, with no Databand server running.

You also configure your `local` environment for Spark to be run, and your `aws_tests` environment for the same purpose.


## Debugging with Remote PyCharm Debugger:
1. Set up a Python debug server in your PyCharm. Follow [this link](https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#remote-debug-config) for the detailed instructions. 
2. In PyCharm, set the host to `localhost` and the port to your arbitrary port in the range of 1024 to 65535.
3. Install the relevant version of `pydevd_pycharm` with pip. PyCharm will tell you the current version number. 

    `pip install pydevd-pycharm~=<version of PyCharm on the local machine>`
    
    for example, `pip install pydevd-pycharm~=191.3490`

4. Set this configuration `debug_pydevd_pycharm_port` in the `run` section to the same port you chose in Step 2.

    The port can be set using the env variable:
    
    `export DBND__RUN__DEBUG_PYDEVD_PYCHARM_PORT=<the wanted port>`
    
    Or on the execution:
    
    `dbnd run dbnd_sanity_check --set-config run.debug_pydevd_pycharm_port=<the wanted port>`

5. Once you run the pipeline, the debugger should catch the run right before the `driver task` executes.