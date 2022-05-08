---
"title": "Generate Airflow DAGs"
---
You can use this mode when you don't want to manage your DAGs via DAG python files, but via configuration or external service like Databand.

# DAGs provider from YAML file
`dbnd-airflow` can load DAGs for you from YAML file.  DBND function `get_dags_from_file` will generate DAGs based on your YAML definition.

1. You need to define the location of the file
```
[scheduler]
config_file=YOUR_DAGS_FILE.yaml
```
2. You need to add the following code to your DAGs. Folder

```python
from dbnd_airflow.scheduler.dags_provider_from_file import get_dags_from_file

# airflow will only scan files containing the text DAG or airflow. This comment performs this function
dags = get_dags_from_file()
if dags:
    globals().update(dags)
```

## DAGs definition
Every element in your yaml file will define a DAG with only one operator which will execute the command line of your choice. You need to define the following fields that will be mapped into DAG and Task definition.

DAG level params
- name will be used as dag_id.
- schedule_interval, catchup will be used at DAG definition.
- start_date, end_date, depends_on_past, and owner will be used as DAG default parameters.

Task level params
- cmd will be executed via BashOperator.
- retries will be used as retries for the operator.

For example:
```
- name: dbnd_sanity_check check!
  cmd: dbnd run dbnd_sanity_check --set task_target_date={{tomorrow_ds}} --task-version now
  schedule_interval: "1 3-19 * * *"
  start_date: 2021-02-15T00:00:00
  catchup: false
  active: true
```

# DAGs provider via Databand Service API

Every job defined at Databand Service will be transformed to DAG in a similar way as it's done at `get_dags_from_file`.  You can manage Jobs via CLI.


> Make sure you are connected to Databand Service [Connecting DBND to Databand (Access Tokens)](doc:access-token)

1.  Use this code to define Apache Airflow DAGs:

```python
from dbnd_airflow.scheduler.dags_provider_from_databand import get_dags_from_databand

# airflow will only scan files containing the text DAG or airflow. This comment performs this function
dags = get_dags_from_databand()
if dags:
    globals().update(dags)
```

2. Manage your jobs via the `dbnd schedule` command.

Commands:
  `job`       Create or edit scheduled job
  `list`      List all scheduled jobs defined at Databand Service
 `pause`    Pause existing scheduled job
 `enable`    Enable scheduled job
 `undelete`  Un-Delete deleted scheduled job
 `delete`    Delete existing scheduled job

You can find parameters for every job with the `--help` flag. For example `dbnd schedule job --help`