# © Copyright Databand.ai, an IBM Company 2024

import re

from datetime import datetime

import freezegun

from dbnd_monitor.component_error import ComponentError


@freezegun.freeze_time("2021-01-01 12:00:00")
def test_component_error_from_exception():
    """
    The test proves the correctness of an exception conversion
    to a ComponentError object.
    """

    try:
        raise ValueError("Test error")
    except Exception as exc:
        result = ComponentError.from_exception(exc)

        assert result.exception_type == "ValueError"
        assert result.exception_message == "Test error"
        assert all(
            text in result.exception_traceback
            for text in [
                "test_component_error.py",
                "line 20, in test_component_error_from_exception",
                "ValueError: Test error",
            ]
        )
        assert result.timestamp == datetime(2021, 1, 1, 12, 0, 0)

        assert re.match(
            r"\[\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}\] (.+): (.+)", str(result)
        )
