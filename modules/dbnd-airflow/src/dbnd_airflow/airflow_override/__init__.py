import os

import airflow
from airflow.utils import configuration, operator_helpers

from dbnd_airflow.airflow_override.databand_dag import DatabandDAG
from dbnd_airflow.airflow_override.databand_task_instance import DbndAirflowTaskInstance
from dbnd_airflow.airflow_override.operator_helpers import context_to_airflow_vars
from dbnd_airflow_windows.airflow_windows_support import enable_airflow_windows_support

__patches = (
    (airflow.models, "TaskInstance", DbndAirflowTaskInstance),
    (airflow.models.taskinstance, "TaskInstance", DbndAirflowTaskInstance),
    (
        airflow.utils.operator_helpers,
        "context_to_airflow_vars",
        context_to_airflow_vars,
    ),
)

__dbnd_dag_tracking = (
    (airflow.models, "DAG", DatabandDAG),
    (airflow.models.dag, "DAG", DatabandDAG),
    (airflow, "DAG", DatabandDAG),
)


def patch_module_attr(module, name, value):
    original_name = "original_" + name
    if getattr(module, original_name, None):
        return
    setattr(module, original_name, getattr(module, name))
    setattr(module, name, value)


def unpatch_module_attr(module, name, value):
    setattr(module, name, getattr(module, "original_" + name))


def patch_models():
    for module, name, value in __patches:
        patch_module_attr(module, name, value)


def unpatch_models():
    for module, name, value in __patches:
        unpatch_module_attr(module, name, value)


def enable_airflow_tracking():
    for module, name, value in __dbnd_dag_tracking:
        patch_module_attr(module, name, value)


def monkeypatch_airflow():
    patch_models()
    if os.name == "nt":
        enable_airflow_windows_support()
