import airflow
from airflow.utils import operator_helpers
from dbnd_airflow.airflow_override.operator_helpers import context_to_airflow_vars

from dbnd._core.utils.object_utils import patch_models


def get_airflow_patches():
    patches = []
    if hasattr(airflow.utils.operator_helpers, "context_to_airflow_vars"):
        patches.append((
        airflow.utils.operator_helpers,
        "context_to_airflow_vars",
        context_to_airflow_vars,
        ))
    return patches


def patch_airflow_modules():
    patch_models(get_airflow_patches())
