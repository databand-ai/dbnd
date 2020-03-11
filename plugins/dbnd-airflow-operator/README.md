# Dbnd Airflow Operator
This plugin was written to provide an explicit way of declaratively passing messages between two airflow operators.

This plugin was inspired by [AIP-31](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-31%3A+Airflow+functional+DAG+definition).
Essentially, this plugin connects between dbnd's implementation of tasks and pipelines to airflow operators.

This implementation uses XCom communication and XCom templates to transfer said messages.
This plugin is fully functional, however as soon as AIP-31 is implemented it will support all edge-cases.

Fully tested on airflow 1.10.X.

