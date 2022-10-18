---
"title": "Tracking dbt"
---

### Overview

dbt jobs can be tracked with Databand in two ways:
1. Using Databand's SDK to track dbt jobs that are triggered by Airflow (or any other Python orchestration)
2. Directly monitoring dbt Cloud using Databand's dbt monitor

### Track dbt runs triggered by Airflow:

You can use Databand to track data from dbt jobs when these jobs are triggered by Airflow by using the `collect_data_from_dbt_cloud` function as shown below.


**Prerequisites**:
1. dbnd SDK installed in airflow environment
2. Airflow is successfully integrated with databand, instructions can be found [here](https://docs.databand.ai/docs/tracking-airflow-dags)
3. dbt cloud account id and dbt cloud api token

1. **Creating Cloud API token-** Please follow the instructions in [dbt Cloud's API documentation](https://docs.getdbt.com/dbt-cloud/api-v2#section/Authentication) to create a dbt Cloud API token. This token will be needed when creating the integration with Databand.
2. **Obtain your dbt Cloud account ID -**  Sign in to your dbt cloud account via your browser. Your dbt cloud account id is the number directly following the `accounts` path component of the URL.


[block:image]
{
  "images": [
    {
      "image": [
        "https://files.readme.io/ef06682-Untitled.png",
        "Untitled.png",
        1200,
        603,
        "#fafafa"
      ]
    }
  ]
}
[/block]

**Tracking dbt cloud runs triggered by Airflow DAGs:**

A common integration of Airflow and dbt cloud is as follows:

1. Airflow run DAG
2. Airflow task triggers a single run of a dbt job in the cloud account
3. Task that polls the cloud API for the run’s status using a run_id to determine how to proceed

In order to track the dbt job using Databand, use Databand’s `collect_data_from_dbt_cloud` function once the job is complete.

Example:
```python
from dbnd import collect_data_from_dbt_cloud

dbt_cloud_run_id = 1234
account_id = 4433
dbt_cloud_api_token = "5a42af03214326778999ccfdbf000044448888bb"

# code for waiting to dbt run to finish....

collect_data_from_dbt_cloud(dbt_cloud_account_id=account_id,
                            dbt_cloud_api_token=dbt_cloud_api_token,
                            dbt_job_run_id=dbt_cloud_run_id)
```

### Tracking dbt Jobs in dbt Cloud
You can use Databand's dbt monitor to track dbt jobs by directly monitoring your dbt Cloud environemnt. This will allow Databand to track your dbt jobs regardless of how they are triggered (Scheduled run, Airflow trigger, manual trigger etc..)

To fully integrate Databand with your dbt Cloud environment:
1. Configure a new [dbt syncer](https://docs.databand.ai/docs/dbt-tracker-configuration) at Databand's Application.
2. After the dbt syncer is configured, you will be able to see your dbt job runs as pipelines in Databand.

[block:callout]
{
  "type": "danger",
  "title": "Tracking Airflow triggered jobs",
  "body": "If you are tracking dbt data from Airflow, using the `collect_data_from_dbt_cloud` function, as explained above, you should NOT configure a dbt syncer to sync dbt job runs from the same account. Tracking the same dbt job runs twice is currently not supported in Databand."
}
[/block]


## `[dbt_monitor]` Configuration Section Parameter Reference
- `prometheus_port` - Set which port will be used for prometheus.
- `interval` - Set the sleep time, in seconds, between fetches, when the monitor is not busy.
- `number_of_iterations` - Set a cap for the number of monitor iterations. This is optional.
- `stop_after` - Set a cap for the number of seconds to run the monitor. This is optional.
