---
"title": "Collected Metadata"
---
This table lists pipeline and dataset metadata collected by Databand and explains how users can control what is collected.
Tracking metadata can be configured using, [SDK Configuration](doc:dbnd-sdk-configuration), [dbnd_config Airflow Connection](doc:integrating-databand-with-airflow#using-airflow-connection-to-configure-airflow-monitor-and-sdk)  Airflow Connection, or Databand UI.  

| Metadata | Default | DBND Config | Airflow Syncer via UI |
|---|---|---|---|
|  **Source Code**      |      disabled     |      ``` [tracking]  track_source_code = true ```     |      Select/Unselect 'Include source code' option at Settings->Airflow Syncers -> Add/Edit Syncer Page     |
|      **Logs**      |      disabled     |      ``` [log]  preview_head_bytes = 8192 preview_tail_bytes = 8192 ```     |      Select/Unselect collect logs option at Settings->Airflow Syncers -> Add/Edit Syncer Page, provide number of KB from head and tail if logs were enabled. (Maximum 8096 KB in each)     |
|      **Errors**      |      enabled     |      ask the Databand team to switch on/off     |      ask the Databand team to switch on/off     |
|      **Airflow XCOM** values     |      disabled     |      ``` [airflow_tracking]  track_xcom_values = true ```     |      not supported in UI, via [`dbnd_config`](doc:integrating-databand-with-airflow): "airflow_tracking": {   "track_xcom_values": true  }     |
|      **return value of Airflow Python Task**     |      disabled     |      ``` [airflow_tracking]   track_airflow_execute_result=true ```     |      not supported in UI, can be done via [`dbnd_config`](doc:integrating-databand-with-airflow) : "airflow_tracking": {   "track_airflow_execute_result": true  }     |
|      **Data Operations**     |      explicit reporting by a user using `log_metric`, `log_dataframe`, and `log_dataset_op` Check [Dataset Logging](doc:dataset-logging) for more info     |          |      |



### Example: Enabling Code and Logs tracking
You  can easily enable Code tracking and Logs tracking by Databand Service by providing the following config
```
[tracking]
track_source_code=True

[log]
preview_head_bytes=15360
preview_tail_bytes=15360
``` 
For Airflow Tracker you can just edit the [Airflow Tracking Configuration](doc:integrating-databand-with-airflow).