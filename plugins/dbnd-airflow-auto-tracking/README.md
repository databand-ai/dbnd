# DBND Airflow Auto Tracking

## The plugin

`dbnd-airflow-auto-tracking` is a plugin for Airflow, enabling dbnd tracking to all DAGs and tasks without any code changes.

### What does it do?

The plugin will go over tasks and modify them if needed.
It uses Airflow's policy mechanism by adding dbnd tracking to policy function.
If the user had already defined its own policy function it will be executed in addition to dbnd tracking.

### Installation

```bash
pip install dbnd-airflow-auto-tracking
```
