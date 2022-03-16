---
"title": "Airflow Orchestration Integration"
---
[Apache Airflow](https://airflow.apache.org/) is an open-source product for programmatically authoring, scheduling, and monitoring data pipelines. It has quickly grown to become a standard for data engineers due to its flexibility, ease of use, and comprehensive UI. 

You can integrate DBND with your existing Apache Airflow environment, and run your pipelines from Airflow, leveraging the Apache Airflow scheduler and using the Apache Airflow UI. The appeal of this approach is that it allows you to leverage the benefits of the Airflow scheduler.

Here are two modes of using DBND with your Airflow DAGs:

## Standalone DBND Pipelines execution via Apache Airflow Executors
This is a unique capability of Databand to run your pipelines with dynamic pipeline creation as a standalone dag as a separate process (similar to the `backfill` command in airflow). In this mode, Databand will use Apache Airflow code. You can use Airflow UI to review the status of the execution, however, Scheduler and Workers are not required.  In this mode [Kubernetes Integration](doc:kubernetes-cluster)  and many other unique features of databand are fully enabled.  You can find more information at [Running DBND Pipelines with Apache Airflow Executors](doc:using-airflow-webserver).

## Generating Airflow DAGs based on YAML definition. 
Using `dbnd-airflow` you can generate Apache Airflow DAGs from YAML file definition, or via Databand Service API (create/enable/delete dags via CLI commands). See more information at [Generate Airflow DAGs](doc:generate-airflow-dags) 

## Decorated tasks in native Airflow DAGs (AIP-31 extension)
Functionally wire DBND tasks to create Airflow DAGs. How it works:
* Use DBND task decorators to seamlessly define tasks using functions 
* Use the DBND pipeline decorator to connect these tasks to form an Airflow DAG
* Use Airflow to run and monitor the pipeline