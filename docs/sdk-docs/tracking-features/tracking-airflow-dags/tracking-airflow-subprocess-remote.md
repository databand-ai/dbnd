---
"title": "Tracking Subprocess/Remote Tasks"
---
Some of the operators cause "remote" execution, so the connection between the Airflow Operator and subprocess execution has to be established.

# Bypassing context of the current Airflow Task Instance into subprocess
Databand will be able to track subprocess/remote process execution in the context of your current Airflow DagRun and TaskInstance if it has the following variables available at the time of execution.

* `AIRFLOW_CTX_DAG_ID` - Airflow DAG ID to associate with a run
* `AIRFLOW_CTX_EXECUTION_DATE` - execution date to associate with a run
* `AIRFLOW_CTX_TASK_ID` -  task ID to associate with a run
* `AIRFLOW_CTX_TRY_NUMBER` - try_attempt of the current run
* `AIRFLOW_CTX_UID` - Airflow instance unique identifier used to distinguish runs performed on different environments

Both JVM And Python SDK supports that parameters

Additionally, subprocess/remote execution should be able to access the Databand service.  The following variables or any of the [SDK Configuration](doc:dbnd-sdk-configuration) methods supported by Databand can be used.

* `DBND__CORE__DATABAND_URL` - Databand tracker URL
* `DBND__CORE__DATABAND_ACCESS_TOKEN` - Databand tracker Access Token
* `DBND__TRACKING` - explicitly enables tracking

Currently, bypassing execution context in addition to regular tracking is automatically supported for the following operators:
  * `EmrAddStepsOperator`
  * `DatabricksSubmitRunOperator`
  * `DataProcPySparkOperator`
  * `SparkSubmitOperator`
  * `BashOperator`
  * `SubDagOperator`

The Databand team is constantly integrating new operators for subprocess metadata tracking. Contact us if you don't see your operator on the list.


## Custom Integration
The best way to inject these variables is to use the already built-in mechanism of your Remote Operator if it has any. For example, you can pass these variables to your Spark Operator via:

<!-- noqa -->
```python
from dbnd._core.utils.uid_utils import get_airflow_instance_uid

MyCustomDataProcPySparkOperator(
         ...
        dataproc_pyspark_properties= {
            "spark.env.AIRFLOW_CTX_DAG_ID": "{{dag.dag_id}}",
            "spark.env.AIRFLOW_CTX_EXECUTION_DATE": "{{ds}}",
            "spark.env.AIRFLOW_CTX_TASK_ID": "{{task.task_id}}",
            "spark.env.AIRFLOW_CTX_TRY_NUMBER": "{{ti.try_attempt}}",

            "spark.env.AIRFLOW_CTX_UID": get_airflow_instance_uid(),

             # static variables, can be set on the cluster itself
            "spark.env.DBND__TRACKING": True,
            "spark.env.DBND__CORE__DATABAND_URL": "https://tracker.databand.ai",
            "spark.env.DBND__CORE__DATABAND_ACCESS_TOKEN=TOKEN"
        }
         ...
    )
```

 If your operator doesn't have a way to provide environment variables in one of the supported formats, you can directly change the command line that you are generating.


<!-- noqa -->
```python
from dbnd._core.utils.uid_utils import get_airflow_instance_uid

airflow_ctx_uid = get_airflow_instance_uid()
cmd =(f"spark-submit  ...  "
           f"--conf spark.env.AIRFLOW_CTX_DAG_ID={context.dag.dag_id}"
           f"--conf spark.env.AIRFLOW_CTX_EXECUTION_DATE={context.execution_date} "
           f"--conf spark.env.AIRFLOW_CTX_TASK_ID={context.task.task_id} "
           f"--conf spark.env.AIRFLOW_CTX_TRY_NUMBER={context.task_instance.try_attempt} "
           f"--conf spark.env.AIRFLOW_CTX_UID={airflow_ctx_uid}"
)
```
