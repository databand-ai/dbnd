# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.tracking.schemas.tracking_info_objects import TaskRunParamInfo
from dbnd._core.utils.data_anonymizers import DEFAULT_MASKING_VALUE
from dbnd.api.serialization.task import TaskRunParamSchema


class TestTaskParamSerialization:
    @pytest.mark.parametrize(
        "parameter_name,parameter_value,expected_masked_value",
        [
            ("my_param", "test123", "test123"),  # No masking should be done
            (
                "password",
                "test123",
                DEFAULT_MASKING_VALUE,
            ),  # Should mask value base on parameter_name exact match
            (
                "user_password",
                "test123",
                DEFAULT_MASKING_VALUE,
            ),  # Should mask value base on parameter_name regex match
            (
                "env",
                "password=1234",
                f"password={DEFAULT_MASKING_VALUE}",
            ),  # Should mask base on string value
        ],
    )
    def test_dump_task_param_no_masking_single(
        self, parameter_name, parameter_value, expected_masked_value
    ):
        # Arrange
        tr_param_schema = TaskRunParamSchema()
        param = TaskRunParamInfo(
            parameter_name=parameter_name, value=parameter_value, value_origin="USER"
        )
        # Act
        serialized_param = tr_param_schema.dump(param).data

        # Assert
        assert serialized_param["parameter_name"] == parameter_name
        assert serialized_param["value_origin"] == "USER"
        # Assert Data Masking
        assert serialized_param["value"] == expected_masked_value

    def test_dump_task_param_no_masking_many(self):
        # Arrange
        tr_param_schema = TaskRunParamSchema()
        params = [
            TaskRunParamInfo(
                parameter_name=f"param_name_{i}",
                value=f"param_value_{i}",
                value_origin="USER",
            )
            for i in range(0, 4)
        ]
        # Act
        serialized_params = tr_param_schema.dump(params, many=True).data
        # Assert
        for ind, serialized_param in enumerate(serialized_params):
            assert serialized_param["parameter_name"] == params[ind].parameter_name
            assert serialized_param["value_origin"] == "USER"
            # Assert Data Masking
            assert serialized_param["value"] == params[ind].value

    def test_dump_task_param_all_masking_many(self):
        # Arrange
        tr_param_schema = TaskRunParamSchema()
        params = [
            TaskRunParamInfo(
                parameter_name=f"param_name_{i}",
                value=f"password={i}",
                value_origin="USER",
            )
            for i in range(0, 4)
        ]
        # Act
        serialized_params = tr_param_schema.dump(params, many=True).data
        # Assert
        for ind, serialized_param in enumerate(serialized_params):
            assert serialized_param["parameter_name"] == params[ind].parameter_name
            assert serialized_param["value_origin"] == "USER"
            # Assert Data Masking
            assert serialized_param["value"] == f"password={DEFAULT_MASKING_VALUE}"

    def test_dump_task_param_masking_many_mix(self):
        # Arrange
        tr_param_schema = TaskRunParamSchema()
        non_sensitive_task_param = TaskRunParamInfo(
            parameter_name="param_name", value="no_mask", value_origin="USER"
        )
        task_param_with_secret_name = TaskRunParamInfo(
            parameter_name="password", value="bla_bla", value_origin="USER"
        )
        task_param_with_sensitive_value = TaskRunParamInfo(
            parameter_name="param11", value="password=456", value_origin="USER"
        )
        # Act
        serialized_params = tr_param_schema.dump(
            [
                non_sensitive_task_param,
                task_param_with_secret_name,
                task_param_with_sensitive_value,
            ],
            many=True,
        ).data
        # Assert
        (
            serialized_non_sensitive_param,
            serialized_sensitive_param_name,
            serialized_sensitive_param_value,
        ) = serialized_params

        assert (
            serialized_non_sensitive_param["parameter_name"]
            == non_sensitive_task_param.parameter_name
        )
        assert (
            serialized_non_sensitive_param["value"] == non_sensitive_task_param.value
        )  # No masking

        assert (
            serialized_sensitive_param_name["parameter_name"]
            == task_param_with_secret_name.parameter_name
        )
        assert serialized_sensitive_param_name["value"] == DEFAULT_MASKING_VALUE

        assert (
            serialized_sensitive_param_value["parameter_name"]
            == task_param_with_sensitive_value.parameter_name
        )
        assert (
            serialized_sensitive_param_value["value"]
            == f"password={DEFAULT_MASKING_VALUE}"
        )
