---
"title": "Tracking Apache Airflow"
---
Databand provides various monitoring, alerting, and analytical functionality that helps you monitor the health and reliability of your Airflow DAGs. Databand allows you to monitor multiple Airflow instances, providing a centralized tracking system for company-wide DAGs.

You can use DAGs tracking functionality for additional visibility into:
* metadata from operators
* task code, logs and errors
* data processing engines (such as [Redshift](doc:tracking-redshift)  and [Spark](doc:tracking-spark-applications) )

You can check what exactly is tracked and how this can be configured at [Data Collection Cheat Sheet](doc:data-collection-cheat-sheet)


## Setting up Airflow Integration

To fully integrate Databand with your Airflow environment:
  1. [Install our runtime tracking ](doc:installing-on-airflow-cluster) `dbnd-airflow-auto-tracking` python package on your Airflow cluster
  2. [Install Airflow Syncer DAG](doc:installing-on-airflow-cluster)
     * Create  `databand_airflow_monitor` DAG in Airflow.  Please create a new file `databand_airflow_monitor.py` with the following dag definition and add it to your project DAGs.
     * Deploy your new DAG and enable it in Airflow UI.


<!-- noqa -->
```python
# databand_airflow_monitor.py
from airflow_monitor.monitor_as_dag import get_monitor_dag
# This DAG is used by Databand to monitor your Airflow installation.
dag = get_monitor_dag()
```
   3.  Configure a [New Airflow Syncer](doc:apache-airflow-sync) at Databand Application.
    * Click on "Integrations" at the left-side menu
    * Click "Add Syncer"

To find this information on Airflow environments (on-prem and managed), refer to our reference guides below:
* [Integrating with Standard Apache Airflow Cluster](doc:installing-on-airflow-cluster#standard-apache-airflow-cluster)
* [Integrating Amazon Managed Workflows Airflow](doc:installing-on-airflow-cluster#aws-managed-workflows)
* [Integrating Google Cloud Composer](doc:installing-on-airflow-cluster#google-cloud-composer)
* [Integrating Astronomer](doc:installing-on-airflow-cluster#astronomer)

# Architecture of Airflow Tracking by Databand

We track all Operators and can capture runtime information from every `.execute()` call within any Airflow Operator.  Everything that happens in the boundaries of the `.execute()` function is tracked. Operator start/end time, user metrics emitted from the code, user exceptions, source code(optional), logs(optional), return value(optional) and a lot of additional information. The moment Databand is integrated with your cluster you can use all functionality from [Tracking Python](doc:python) inside your Operator implementation.

 In addition, there is a component called [Airflow Syncer](doc:apache-airflow-sync)  that syncs execution airflow metadata from the Airflow database.

 You can check what exactly is tracked and how this can be configured at [Data Collection Cheat Sheet](doc:data-collection-cheat-sheet)

 Some of the operators cause "remote" execution, so the connection between Airflow Operator and sub-process execution has to be established.  Databand supports multiple Spark-related operators as well as Bash and some other Operators. See [Tracking Sub-Process/Remote Tasks)](doc:tracking-airflow-subprocess-remote)  for more information.

![Databand Architecture Airflow Sync As DAG.drawio.png](https://files.readme.io/677174f-Databand_Architecture_Airflow_Sync_As_DAG.drawio.png)
