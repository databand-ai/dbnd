# © Copyright Databand.ai, an IBM Company 2024

from random import choice

import pytest

from dbnd_monitor.component_error import ComponentError, ReportErrorsDTO


def test_component_error_model_dump():
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
    dumped = component_error.model_dump()

    # Assert:
    # the new instance of the ComponentError created from the model's dump
    # must be equal to the original instance
    assert ComponentError(**dumped) == component_error


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


def test_report_errors_dto_model_dump():
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
    dumped = report_errors_dto.model_dump()

    # Assert:
    # the new instance of the ReportErrorsDTO created from the model's dump
    # must be equal to the original instance
    assert ReportErrorsDTO(**dumped) == report_errors_dto

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
                "timestamp": error_0.timestamp,
            },
            {
                "exception_type": "AttributeError",
                "exception_body": "1: Error",
                "traceback": "AttributeError: 1: Error\n",
                "timestamp": error_1.timestamp,
            },
        ],
        "is_error": True,
    }
