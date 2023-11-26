# © Copyright Databand.ai, an IBM Company 2022

from dbnd.providers.dbt.dbt_core import collect_data_from_dbt_core


# backward compatibility for import like:
#   from dbnd._core.tracking.dbt import collect_data_from_dbt_core
#   in the future, please use from dbnd import collect_data_from_dbt_core
__all__ = ["collect_data_from_dbt_core"]
