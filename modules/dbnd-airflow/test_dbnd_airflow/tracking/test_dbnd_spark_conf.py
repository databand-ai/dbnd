# © Copyright Databand.ai, an IBM Company 2024

from typing import Any, Dict
from unittest.mock import patch

import pytest

from dbnd_airflow.tracking.dbnd_spark_conf import generate_spark_properties


@pytest.mark.parametrize(
    "domain,additional_props,is_verbose",
    [
        ("spark.env", {}, False),
        ("spark.env", {}, True),
        (
            "spark.yarn.appMasterEnv",
            {"SPARK_ENV_LOADED": "1", "DBND_HOME": "/tmp/dbnd"},
            False,
        ),
        (
            "spark.yarn.appMasterEnv",
            {"SPARK_ENV_LOADED": "1", "DBND_HOME": "/tmp/dbnd"},
            True,
        ),
        ("spark.kubernetes.driverEnv", {}, False),
        ("spark.kubernetes.driverEnv", {}, True),
    ],
)
def test_generate_spark_properties(
    domain: str, additional_props: Dict[str, Any], is_verbose: bool
):
    with patch("dbnd_airflow.tracking.dbnd_spark_conf.is_verbose") as is_verbose_patch:
        is_verbose_patch.return_value = is_verbose
        generate_spark_properties(domain, additional_props)
        expected_result = {
            f"{domain}.DBND__TRACKING": True,
            f"{domain}.DBND__ENABLE__SPARK_CONTEXT_ENV": True,
            **{f"{domain}.{k}": v for k, v in additional_props.items()},
        }
        if is_verbose:
            expected_result.update({f"{domain}.DBND_VEBOSE": True})
