---
"title": "CLI for Managing Airflow Syncers"
---
In addition to being able to control your [Airflow Syncers ](doc:apache-airflow-sync) via UI, you can create a new Airflow Syncer in the databand using the command line. 

Make sure, prior to running, that you have `DATABAND_URL` and `DATABAND_ACCESS_TOKEN` set to the correct values. You can read more about how to do that on the [Connecting DBND to Databand](https://docs.databand.ai/docs/access-token) page.



## **Add** 
To create a new Airflow syncer you need to run the `dbnd airflow-sync add` command with the following arguments:

| Field | Description | Required / Optional |
|---|---|---|
| **--url** | Airflow *server* url | **Required** |
| **--external-url** | External url for instance | Optional. Default: None |
| **--name** | Name for the syncer | Optional. Default: None |
| **--fetcher** [web\|db] | Set always to db, unless you are not using databand_airflow_monitor dag | Optional Default: db |
| **--env** | The environment name for the syncer | Optional. Default: None |
| **--include-sources** (flag) | Monitor source code for tasks | Optional. Default: False |
| **--dag-ids** | List of specific dag ids (separated with comma) that monitor will fetch only from them | Optional. Default: None |
| **--last-seen-dag-run-id** | Id of the last dag run seen in the Airflow database | Optional |
| **--last-seen-log-id** | Id of the last log seen in the Airflow database | Optional |
| **--generate-token** | Generate access token for the syncer, value is token lifespan | Optional. Default: None |
| **--config-file-output** | Store syncer config json to file | Optional. Default: - |
| **--with-auto-alerts** (flag) | Create syncer with auto alerts config | Optional. Default: False |
| **--include-logs-bytes-from-head** [0-8096] | Include the number of bytes from the head of the log file | Optional. Default: 0 |
| **--include-logs-bytes-from-end** [0-8096] | Include the number of bytes from the end of the log file | Optional. Default: 0 |
| **--dag-run-bulk-size** | DAG run bulk size for the syncer | Optional. Default: None |
| **--start-time-window** | Start time window for the syncer (X days backwards) | Optional. Default: None |


**Add example:** 
```  bash
dbnd airflow-sync add --url http://airflow:8082 --fetcher db --name my_airflow
```

## **Edit:**
You can use all params from add, excluding **--generate-token** and **--config-file-output**

| Field | Description | Required / Optional |
|---|---|---|
| **--tracking-source-uid** | Tracking source uid of the edited airflow syncer. (you can get this with the "list" command) | **Required** |

 **Edit example** 
```  bash
 dbnd airflow-sync edit --tracking-source-uid c9cc2928-6966-11ac-ba39-adde48001122 \
          --url http://airflow:8082 --fetcher db --name my_airflow
```