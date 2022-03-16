---
"title": "Alerting CLI"
---
In addition to being able to control alerting rules via UI, you can manage alerting rules in the databand using the command line.

Make sure, prior to running, that you have DATABAND_URL and DATABAND_ACCESS_TOKEN set to the correct values. You can read more about how to do that on the [Connecting DBND to Databand page](doc:access-token).


> ⚠️ Supported Alert Types
> This version of CLI only supports individual pipeline alerts

## List

This command lists all existing alert definitions. 

**Paramеters**

* `—pipeline` or `-p` Name of the pipeline on which to show alerts.

** List Example **

```bash
dbnd alerts list --pipeline my_pipeline
```



## Create

Create or update the alert definition

**Parameters:**

* `--update` or `uid`  or  `-u` UID of an existing alert to update. Used in case of update.
* `--pipeline` or `-p` Pipeline name on which alert will be created.
* `--pipeline-id` Pipeline id on which alert will be created.
* `—severity` or `-s` Alert severity, one of the following values are supported - `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`.
 

###  Run duration of pipeline in seconds.
    `run-duration`
**Parameters**
 * `—op` or `-o` Operator one of `==`, `!=`, `<=`, `>=`, `<`, `>`, `ML`, `range`.
`ML` operator requires two additional params:
  * `--look-back` or `-lb` Look Back - number of runs used to train anomaly alerts.
  * `--sensitivity” or `-sn` Sensitivity of anomaly detection alerts.
 
`range` operator requires two additional params:
* `--baseline` or `-b` Base number from which range alert will be measured.
* `--range` or `-r` Percent of baseline to include in range alerts.
* `--value` or `-v` Numerical value of the run-duration. Required in numerical operators `==`, `!=`, `<=`, `>=`, `<`, `>`. Do not provide for `ML` or `range` alert type.

Run Duration Alert Examples: 

```bash
dbnd alerts create --pipeline my_pipeline --severity MEDIUM run-duration --op '==' -v 100
dbnd alerts create --pipeline my_pipeline --severity HIGH run-duration --op ML --look-back 10 --sensitivity 4
dbnd alerts create --pipeline my_pipeline --severity HIGH run-duration --op range --baseline 10 --range 4
```


    
###  State of the run to alert on.
     `run-state`
**Parameters**:
 * `--value` or `-v` String value of the run state, one of `running`, `success`, `failed`, `shutdown`, `cancelled`. Note: operator is hardcoded to “==” and shouldn’t be provided.
    
Example for Run State Alert

```bash
dbnd alerts create --pipeline my_pipeline --severity MEDIUM run-state -v failed
```

    
###  Duration in seconds from the last successful run from a pipeline.
  `ran-last-x-seconds`
**Parameters**:   
* `--value` or `-v` Numerical value of last ran seconds. Note that the operator is hardcoded to `>` and shouldn’t be provided.
 
Example

```bash
dbnd alerts create --pipeline my_pipeline --severity CRITICAL ran-last-x-seconds -v 120
```


###  State of the task run to alert on.
    `task-state`
**Parameters**

*`—value` or `-v` - String value of task state, one of `scheduled`, `queued`, `running`, `success`, `failed`, `cancelled`, `shutdown`,  `upstream_failed, skipped, up_for_retry, removed`. Note that operator is hardcoded to “==” and shouldn’t be provided.
 
Example

```bash
dbnd alerts create --pipeline my_pipeline --severity MEDIUM --task my_pipeline_task task-state -v cancelled
```

    
###  Alert on metric value
    `custom-metric`
**Parameters**
    
* `--metric-name` or `-m` Custom metric name to alert.
* `--str-value` Boolean param indicating whether metric has a float of string values.
* `--op` or `-o` Operator of the metric. One of `==`, `!=`, `<=`, `>=`, `<`, `>`, `ML`, `range`. if `--str-value` set to false, otherwise operator can take only  take `==` or `!=` values.
* `--value` or  `-v`.  Numerical value of the metric corresponding `==`, `!=`, `<=`, `>=`, `<`, `>` operators, Or String value corresponding `==`, `!=` operators based on `--str-value` param. Required only on the listed above operators except `ML` or `range`.


ML operator requires two additional params:
* `--look-back` or `-lb` Look Back number of runs used to train anomaly alerts.
* `--sensitivity` or `-sn` Sensitivity of anomaly detection alerts.

Range operator requires two additional params:
* `--baseline` or `-bl` Base number from which range alert will be measured.
* `--range` or `-r` Percent of baseline to include in range alerts.
    
Example
 
```bash
dbnd alerts create --pipeline my_pipeline --severity CRITICAL --task my_pipeline_task custom-metric --metric-name my_metric --op '<=' -v 100 --str-value false
```



## Delete

Delete individual alert definition, all alert definitions for a specific pipeline, or all existing alert definitions.

** Parameters **

* `--uid`  or `-u` Alert UID to delete.
* `--wipe`  Delete all alerts.
* `--pipeline`  or `-p` Pipeline name on which alerts will be deleted. Can be used with `--name` or  `-n` indicating alert custom name.

**Delete Examples**:

```bash
dbnd alerts delete --uid my_alert_uid
dbnd alerts delete --pipeline my_pipeline
dbnd alerts delete --wipe
```