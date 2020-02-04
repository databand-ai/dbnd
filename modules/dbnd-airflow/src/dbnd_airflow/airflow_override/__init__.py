import os

import airflow
from airflow.utils import operator_helpers
from dbnd_airflow.airflow_override.operator_helpers import context_to_airflow_vars

__patches = (
  #   (airflow.models, "TaskInstance", DbndAirflowTaskInstance),
  #   (airflow.models.taskinstance, "TaskInstance", DbndAirflowTaskInstance),
    (
        airflow.utils.operator_helpers,
        "context_to_airflow_vars",
        context_to_airflow_vars,
    ),
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


def monkeypatch_airflow():
    patch_models()
