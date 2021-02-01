# Dbnd Airflow Export plugin

## The plugin

`dbnd-airflow-export-plugin` is a plugin for Airflow system, enables you to fetch data from Airflow database and DAG folder.
This Airflow side module is one of two components allows you to observe your Airflow data with `Databand` system.

### What does it do?

The plugin exposes a REST Api within `GET` `/export_data` which, expects `since` (utc).
This api returns json with all the relevant information scraped from airflow system.

### Configuring Airflow instance
To improve performance and reduce the runtime overhead on airflow database, we advise to add an index on end_date column to task_instance and dag_run tables i.e.

```
   CREATE INDEX dbnd_ti_end_date_ind ON task_instance (end_date);
   CREATE INDEX dbnd_dr_end_date_ind ON dag_run (end_date);
```

### Installation

In order to install `dbnd-airflow-export-plugin` we are using Airflow plugin system.

```bash
pip install databand[airflow-export]
```
