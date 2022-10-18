---
"title": "Python SDK Configuration"
---
Python DBND SDK uses its own configuration system that enables you to set and update your configuration in the most usable way. It consists of:

* Configuration files
* Environment Variables
* Code
* External Configs (for example Airflow Connection)

## Environment Variables
You can set any system parameter by using environment variables.
For example, you can override the `databand_url` parameter under the `core` section by setting a value for the  `DBND__CORE__DATABAND_URL` environment variables.
Similarly, you can construct environment variables for other configuration parameters by using `DBND__<SECTION>__<KEY>` format.


## Configuration files in DBND
DBND loads configuration information sequentially from the following configuration files:


| File loading priority | File Location                    | File Description                                                                                          |
|-----------------------|----------------------------------|-----------------------------------------------------------------------------------------------------------|
| 1                     | $DBND_LIB/databand-core.cfg      | Provides the default core configuration of the system that cannot be changed.                             |
| 2                     | $DBND_SYSTEM/databand-system.cfg | Provides middle layer configuration. We suggest to use this file for project infrastructure configuration |
| 3                     | $DBND_HOME/project.cfg           | Provides a project configuration. Should be used for configuring "user" facing parts of the project.      |
| 4                     | $USER_HOME/.dbnd/databand.cfg    | Provides system user configuration.                                                                       |
| 5                     | Custom location                  | You can also create additional configuration files by using `DBND__CONF__FILE` environment variable.      |


> ✅ **Note**
> Configuration information available in files specified in lower configuration layers overrides the configuration specified in the top layers.

For example, you have config key A specified in the **$DBND_SYSTEM/databand-system.cfg**. If in **$DBND_HOME/project.cfg** the configuration of A is specified, then DBND uses the configuration specified in the lower configuration layer,  i.e. **$DBND_HOME/project.cfg**.
See [ConfigParser.read](https://docs.python.org/3/library/configparser.html).


## Environment variables in Configuration files
You can use `$DBND_HOME`, `$DBND _LIB` or `$DBND_SYSTEM` in your configuration file, or any other environment variable:

## How to change the configuration for a specific section of the code:

You can use the config context manager to set up the configuration in code:
```python
from dbnd import config
with config({"section": {"key": "value"}}):
    pass
```

You can also load configuration from a file:

<!-- noqa -->
```python
from dbnd import config

from dbnd._core.configuration.config_readers import read_from_config_file
with config(read_from_config_file("/path/to/config.cfg")):
    pass
```


### Q: How do I pass list parameter in .cfg file?
A: The syntax is as follows:
```ini
[some_section]
list_param = [1, 2, 3, "str1"]
```

### Q: How do I pass dictionary parameter in .cfg file?
A: The syntax is as follows:
```ini
[some_section]
dict_param = {"key1": "value1", 255: 3}
```

### Q: Is it possible to have multiple configuration files? For instance, if there are different use cases or some default variables are overridden in prod or test.
A: Yes, to specify extra file, you can set an environment variable:
`export DBND_CONFIG=<extra_file_path>`

### Q: Can I use multiple extra config files?
A: Yes, `-- conf` and `DBND__DATABAND__CONF` variables support multiple files (as a list of files separated by comma).

### Q: How to control the output type of `dbnd_config`?
A: The `dbnd_config` is a ‘dict’-like object that only stores the mapping to value. To control the output type of `dbnd_config.get` ("section", "key"), you can use `getboolean` , `getint`, or `getfloat` (for permeative types).



## Tracking Store
The Python DBND SDK is using the `tracking system` to report the state of the Runs or Tasks to the Databand webserver.
Errors will happen while trying to report important information, and they can cause invalid states for the Runs you see in the Databand webapp.

The `tracking system` uses different `tracking store`, and each reports the information to a different location, for example:
1. Web tracking-store - reports to Databand webserver.
2. Console tracking-store - writes the events to the console.

To control the behavior of the `tracking system` when there are errors, use the following configurations under the `core` section:
* `remove_failed_store` - Removes a `tracking store` in case of multiple fails; default value = `false`.
* `max_tracking_store_retries` - Maximal amounts of retries allowed for a single `tracking store` call if it fails; default value = `2`.
* `tracker_raise_on_error` - Stops the run with an error if there is a critical error on tracking like failing to connect the web-server; default value = `true`.



## Controlling parameters logging within Decorated functions

  performant logging ("zero-computational-cost") option helps users save computational resources and protect against reporting of sensitive data (like full data previews).

Logging data processes and full data quality reports in DBND can be resource-intensive. However, explicitly turning off all calculations for `log_value_size`, `log_value_schema`, `log_value_stats`, `log_value_preview`, `log_value_preview_max_len`, `log_value_meta` (the former approach) will result in valuable metrics not being tracked at all. To help users better manage performance and visibility needs, it's now possible to turn selectively throttle the calculations and logging of metadata through a zero-computational-cost approach.

Using a new configuration, you can decide if want to log certain information or not with the help of `value_reporting_strategy`. `value_reporting_strategy` changes nothing in your code, but acts as a guard (or fuse) before the value calculation code gets to execution:

``` ini
[tracking]
value_reporting_strategy=SMART
```

These are the 3 options available:

*  **"ALL => no limitations"** - no restrictions for logging. All the log_value_ types are going to be on: `log_value_size`, `log_value_scheme`, `log_value_stats`, `log_value_preview`, `log_value_preview_max_len`, `log_value_meta`.

*  **"SMART => restrictions on lazy evaluation types”** - with SMART, for types like Spark calculations are only performed as required. The calculations for lazy evaluation types are restricted and even if you have `log_value_preview` set to True, with the SMART strategy on, the Spark previews will not be logged.

*  **"NONE => limit everything"** that doesn’t allow logging of anything expensive or potentially problematic. This can be useful if you have some or many values that constitute private and sensitive information, and you don’t want it to be logged.

Most users will benefit from using the SMART option for logging.

## `[tracking]` Configuration Section Parameter Reference
- `project` - Set the project to which the run should be assigned. If this is not set, the default project is used. The tracking server will select a project with `is_default == True`.
- `databand_external_url` - Set tracker URL to be used for tracking from external systems.
- `log_value_size` - Should this calculate and log the value's size? This can cause a full scan on non-indexable distributed memory objects.
- `log_value_schema` - Should this calculate and log the value's schema?
- `log_value_stats` - Should this calculate and log the value's stats? This is expensive to calculate, so it might be better to use log_stats on the parameter level.
- `log_value_preview` - Should this calculate and log the value's preview? This can be expensive to calculate on Spark.
- `log_value_preview_max_len` - Set the max size of the value's preview to be saved at the DB. The max value of this parameter is 50000
- `log_value_meta` - Should this calculate and log the value's meta?
- `log_histograms` - Enable calculation and tracking of histograms. This can be expensive.
- `value_reporting_strategy` - Set the strategy used for the reporting of values. There are multiple strategies, each with different limitations on potentially expensive calculations for value_meta. `ALL` means there are no limitations. `SMART` means there are restrictions on lazy evaluation types. `NONE`, which is the default value, limits everything.
- `track_source_code` - Enable tracking of function, module and file source code.
- `auto_disable_slow_size` - Enable automatically disabling slow previews for Spark DataFrame with text formats.
- `flatten_operator_fields` - Control which of the operator's fields would be flattened when tracked.
- `capture_tracking_log` - Enable log capturing for tracking tasks.

