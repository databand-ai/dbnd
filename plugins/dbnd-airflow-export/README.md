# Dbnd-Airflow Syncing mechanism

## dbnd-airflow-sync (an Airflow plugin) 

`dbnd-airflow-sync` is a plugin for Airflow system, enables you to fetch data from Airflow database and DAG folder. 
This Airflow side module is one of two components allows you to sync your Airflow data into `Databand` system.
  
### What does it do?
The plugin exposes a REST Api within `GET` `/export_data` which, expects `since` (utc) and `period` (int) in minutes.
This api returns json with all the relevant information scraped from airflow system.

### Installation
In order to install `dbnd-airflow-sync` we are using Airflow plugin system.

#### Easy installation (recommended):
Copy the plugin file into airflow plugins folder in you project (Airflow will automatically look for your plugins in this folder when startup)
```bash
mkdir $AIRFLOW_HOME/plugins
cp dbnd-airflow-sync/src/dbnd_airflow_export/dbnd_airflow_export_plugin.py $AIRFLOW_HOME/plugins/
```

#### Setup tools:
You can also install `dbnd-airflow-sync` using setup tools.
```bash
cd dbnd-airflow-sync
pip install -e .
```

## dbnd-airflow-sync (a Databand module) 

`dbnd-airflow-sync` is a stand-alone module for Databand system, enables you to load data from Airflow server and import it into `Databand` system. 
This Databand side module is one of two components allows you to sync your Airflow data into `Databand` system.
 

### Installation with setup tools
```bash
cd modules/dbnd-airflow-sync
pip install -e .
```
 
### Usage
`dbnd airflow-monitor`


### Configuration
You can configure your syncing variables inside `airflow_sync.cfg`

```cfg
[core]
interval = 10 ; Time in seconds to wait between each fetching cycle 
fetcher = web ; Fetch method. Data can be fetched directly from db or through rest api [web\db] 
include_logs = True ; Whether or not to include logs (might be heavy)

# Fetch period in mins
fetch_period = 60 ; Time in minutes for window fetching size (start: since, end: since + period)


[web]
url = http://localhost:8080/admin/data_export_plugin/export_data ; When using fetcher=web, try this url

[db]
sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db ; When using fetcher=db, use this sql connection string
dag_folder =  /usr/local/airflow/dags ; When using fetcher=db, this is the dag folder location
```
