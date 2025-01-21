# © Copyright Databand.ai, an IBM Company 2024

from datetime import datetime
from random import choice

import pytest

from dbnd_monitor.error_handling.component_error import ComponentError, ReportErrorsDTO


def test_component_error_dump():
    """
    The test proves the equality of instances created manually and recreated
    from the dictionary representing the ComponentError model, what means
    serialization/deserialization works as expected
    """
    # Arrange:
    # create the refference instance
    component_error = ComponentError.from_exception(ValueError("0: Error"))

    # Act:
    # create the dictionary representing the model, i.e. model's dump
    dumped = component_error.dump()

    # Assert:
    # `dumped` dict "timestamp" value must be a type of str
    assert isinstance(dumped["timestamp"], str)

    # Act
    # Cast the "timestamp" value back to the datetime instance
    dumped["timestamp"] = datetime.fromisoformat(dumped["timestamp"])

    # Assert:
    # the new instance of the ComponentError created from the model's dump
    # must be equal to the original instance
    assert ComponentError(**dumped) == component_error


def test_component_error_equality():
    """
    The test proves the equality of `hash`es of two ComponentError instances
    even when they are created at different times
    """

    # Arrange
    exc_1 = ValueError("0: Error")
    ce_1 = ComponentError.from_exception(exc_1)

    exc_2 = ValueError("0: Error")
    ce_2 = ComponentError.from_exception(exc_2)

    # Assert
    # Exceptions are not the same and ComponentErrors created from them also
    assert exc_1 != exc_2
    assert ce_1 != ce_2
    # Timestamps are not the same
    assert ce_1.timestamp != ce_2.timestamp

    # Hash'es are the same though
    assert hash(ce_1) == hash(ce_2)


@pytest.mark.parametrize(
    "param",
    [
        pytest.param(0, id="int"),
        pytest.param("ValueError('0: Error')", id="str"),
        pytest.param((ValueError, "0: Error"), id="tuple"),
    ],
)
def test_component_error_raises_value_error(param):
    """
    The test proves that the `from_exception` method raises a ValueError
    when the input is not an exception instance
    """
    with pytest.raises(ValueError) as value_error:
        ComponentError.from_exception(param)

    assert value_error.match("Expected an exception instance")


def test_report_errors_dto_dump():
    """
    The test proves the equality of instances created manually and recreated
    from the dictionary representing the ReportErrorDTO model, what means
    serialization/deserialization works as expected
    """

    external_id = choice(["test_external_id", None])
    error_0 = ComponentError.from_exception(ValueError("0: Error"))
    error_1 = ComponentError.from_exception(AttributeError("1: Error"))

    # Arrange:
    # create the refference instance
    report_errors_dto = ReportErrorsDTO(
        tracking_source_uid="test_tracking_source_uid",
        external_id=external_id,
        component="test_component",
        errors=[error_0, error_1],
    )

    # Act:
    # create the dictionary representing the model, i.e. model's dump
    dumped = report_errors_dto.dump()

    # Assert:
    # Fully populated ReportErrorsDTO model
    assert dumped == {
        "tracking_source_uid": "test_tracking_source_uid",
        "external_id": external_id,
        "component": "test_component",
        "errors": [
            {
                "exception_type": "ValueError",
                "exception_body": "0: Error",
                "traceback": "ValueError: 0: Error\n",
                "timestamp": error_0.timestamp.isoformat(),
            },
            {
                "exception_type": "AttributeError",
                "exception_body": "1: Error",
                "traceback": "AttributeError: 1: Error\n",
                "timestamp": error_1.timestamp.isoformat(),
            },
        ],
        "is_error": True,
    }

    # Act:
    # In order to recreate an instance of the ReportErrorsDTO from the dictionary,
    # `errors` must be converted back to a list of ComponentError instances
    # and all timestamps recreated from ISO formatted strings

    dumped["errors"] = [
        ComponentError(
            exception_type=dump["exception_type"],
            exception_body=dump["exception_body"],
            traceback=dump["traceback"],
            timestamp=datetime.fromisoformat(dump["timestamp"]),
        )
        for dump in dumped["errors"]
    ]

    # Assert:
    # the new instance of the ReportErrorsDTO created from the model's dump
    # must be equal to the original instance
    assert ReportErrorsDTO(**dumped) == report_errors_dto


def test_report_errors_dto_when_external_id_missed():
    """
    The test proves that the `ReportErrorsDTO` model raises a TypeError, when
    'external_id' is not provided
    """
    with pytest.raises(TypeError) as exc:
        ReportErrorsDTO(tracking_source_uid="test", component="test", errors=[])

    assert str(exc.value) == (
        "ReportErrorsDTO.__init__() missing 1 required positional argument: "
        "'external_id'"
    )
