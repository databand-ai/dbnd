# Dbnd Airflow Export plugin

## The plugin

`dbnd-airflow-export-plugin` is a plugin for Airflow system, enables you to fetch data from Airflow database and DAG folder.
This Airflow side module is one of two components allows you to observe your Airflow data with `Databand` system.

### What does it do?

The plugin exposes a REST Api within `GET` `/export_data` which, expects `since` (utc).
This api returns json with all the relevant information scraped from airflow system.

### Installation

In order to install `dbnd-airflow-export-plugin` we are using Airflow plugin system.

```bash
pip install databand[airflow-export]
```
