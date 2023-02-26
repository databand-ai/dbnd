# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.utils.task_utils import get_project_name_safe


@pytest.mark.parametrize(
    "explicit_project_name, config_project, expected_project_name",
    [
        ("project_name", "config_project", "project_name"),
        ("project_name", None, "project_name"),
        ("project_name", "config_project", "project_name"),
        (None, None, None),
    ],
)
def test_project_name_precedence(
    explicit_project_name, config_project, expected_project_name, databand_test_context
):
    assert (
        get_project_name_safe(
            project_name=explicit_project_name, task_or_task_name="test_func"
        )
        == expected_project_name
    )
