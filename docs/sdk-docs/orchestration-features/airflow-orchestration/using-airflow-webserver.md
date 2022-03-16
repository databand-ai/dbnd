---
"title": "Running DBND Pipelines with Apache Airflow Executors"
---
## Installing Apache Airflow for DBND usage
You should install apache-airflow into your virtualenv first. 
Please follow this guide for more detailed instructions: https://airflow.apache.org/docs/apache-airflow/1.10.10/installation.html

We recommend using `apache-airflow`  at version `1.10.10`. You can use the following command to install it 
```
CURRENT_PY_VERSION = $(shell python -c "import sys; print('{0}{1}'.format(*sys.version_info[:2]))") 

pip install apache-airflow[postgres] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.10/constraints-${CURRENT_PY_VERSION}.txt"
```
Make sure you use one of the DB backends for Airflow. We recommend using Postgres.  Make sure you have followed this guide [initialized your Postgres DB](https://airflow.apache.org/docs/apache-airflow/1.10.10/howto/initialize-database.html
 
## Enable Airflow Executors
You can configure your default executor at `[run] task_executor_type`, `dbnd` will automatically use Airflow `LocalExecutor`( runs tasks in parallel) the moment `--parallel` switch is provided. If you use [Kubernetes Integration](doc:kubernetes-cluster)  as a remote engine, Apache Airflow `KubernetesEngine` is going to be automatically selected. You can see you, current executor, at the last line of your Run banner.


## Using Airflow's native web UI to view DBND pipelines.
You can use Airflow's native web UI to view DBND pipelines. This requires installing a DBND plugin for Airflow called `airflow-versioned-dag`. Install with the following command: `pip install dbnd-airflow-versioned-dag`.
 
The `airflow-versioned-dag` plugin enables you to view unique versions of the DAG graph of every execution so that all runs are discoverable. This makes it easier to make iterations, improvements, and debug. On its own, Airflow will save the DAG graph of only the latest execution in the Airflow database.

The best way to use Airflow Server is to deploy is using one of the available options described at Apache Airflow documentation site. 

## Airflow Home
Usually user control `${AIRFLOW_HOME}` location by himself. In case you would like to have a configuration as part of your project, you'll need to make sure that that variable is properly assigned.  `dbnd-airflow` takes an extra step, and if it finds `${DBND_HOME}/.airflow` or `${DBND_SYSTEM}/airflow`, it will assign `${AIRFLOW_HOME}` value to be one of that folders (for example `YOUR_PROJECT/.dbnd/airflow`).  You can create one of these folders and airflow will automatically populate them with default configuration files.

## Scheduling DBND pipelines in Apache Airflow Scheduler
Define a DBND pipeline that’s executed by Airflow’s scheduler as a task. How it works:
* Create a DBND pipeline
* Contain it as an operator (Bash, Python, etc.)
* Use Airflow to run and monitor the pipeline
See more at [Generate Airflow DAGs](doc:generate-airflow-dags) 

>ℹ️  Enhanced Observability with Databand Application
> For users who want an extended interface, alerting, and more observability functionality, Databand provides a premium web service that can be paired with DBND or directly with your existing Airflow deployment. Reach out to the team at contact@databand.ai to learn more.