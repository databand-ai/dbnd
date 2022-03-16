---
"title": "Installing Python SDK"
---
# Installing DBND

From the command line, run the following command:
```shell
pip install databand
```

The DBND PyPI basic package installs only packages required for getting started. Behind the scenes, DBND does conditional imports of operators that require extra dependencies.

Whether you are looking to track your pipeline metadata, or if you want to orchestrate pipelines, you may want to install DBND plugins for integrating with third-party tools.

| See [Connecting DBND to Databand](doc:access-token) to learn how to connect SDK integrations into your Databand Application.


## Installing Plugins

Run the following command to install any of the plugins listed in the tables below. For example:
```shell
pip install dbnd-spark dbnd-airflow
```

You can use bundled installation via `databand[plugin-slug]`
```shell
pip install databand[spark,airflow]
```

## Plugins for tracking

| Plugin name                | Description                                                                                            |
|----------------------------|--------------------------------------------------------------------------------------------------------|
| dbnd-airflow               | Enables monitoring of Airflow DAGs by DBND. Supported versions are 1.10.7-2.2.3                        |
| dbnd-airflow-auto-tracking | Enables automatic tracking for Airflow DAGs.                                                           |
| dbnd-airflow-export        | Enables exporting of Airflow DAGs metadata from Airflow Web UI (used by dbnd-airflow-monitor service). |
| dbnd-luigi                 | Enables integration with Luigi. Monitors Luigi pipelines execution.                                    |
| dbnd-spark                 | Required for Spark DataFrame observability features.                                                   |
| dbnd-mlflow                | Enables integration with MLflow (submitting all metrics via MLFlow bindings).                          |
| dbnd-postgres              | Enables integration with the Postgres database.                                                        |
| dbnd-redshift              | Enables integration with the Redshift database.                                                        |
| dbnd-snowflake             | Enables integration with the Snowflake database.                                                       |


### SDK version in different parts of the system. 
It's strongly advised that you use the same SDK version across all components in communication - for example, an Airflow DAG Python environment and a Spark cluster environment.


# System Requirements
You can install DBND on the following operating systems:
  * Linux (*Recommended for production*)
  * macOS
  * Windows

## Software requirements
Before installing DBND, make sure you have the following software installed:

| Mandatory            | Software                                                                                                       | Additional information                                                                                                                                                                             |
|----------------------|----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Mandatory            | [Python 3.6 and later] (https://www.python.org/downloads/)                                                     | We recommend using Python 3.7 as it's our main supported version.                                                                                                                                  |
| Strongly recommended | * [Virtualenv](https://virtualenv.pypa.io/en/latest/) *or *   * [Conda](https://docs.conda.io/en/latest/)      | Each of these virtual environments has its own Pip installation that is required.  If you are an Anaconda user, run  `conda install pip`,  and follow the installation instructions for pip users. |
| Windows only         | [Microsoft Build Tools 2019](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019) |                                                                                                                                                                                                    |
| Linux only           | [setproctitle 1.1.10](https://pypi.org/project/setproctitle/)                                                  | Run `sudo apt-get install python-setproctitle`                                                                                                                                                     |


## Supported platforms and libraries

| Platform/Library     | Supported versions | Notes |
|----------------------|--------------------|-------|
| Apache Airflow       | 1.10.7-2.2.3       |       |
| Strongly recommended | 0.24-1.1.5         |       |