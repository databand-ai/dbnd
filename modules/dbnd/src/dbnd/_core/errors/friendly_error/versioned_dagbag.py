# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.errors import DatabandError


def failed_to_load_versioned_dagbag_plugin(exc):
    return DatabandError(
        "Failed to switch to versioned dagbag!", exc, show_exc_info=True
    )


def failed_to_retrieve_dag_via_dbnd_versioned_dagbag(exc):
    return DatabandError(
        "Failed to retrieve DAG from DbndDagBag!", exc=exc, show_exc_info=True
    )
