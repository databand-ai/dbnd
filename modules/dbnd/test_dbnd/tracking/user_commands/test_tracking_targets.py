import pytest

from more_itertools import one

from dbnd import log_target_operation, task
from dbnd._core.constants import DbndTargetOperationStatus, DbndTargetOperationType
from dbnd.testing.helpers_mocks import set_tracking_context
from targets import target
from targets.value_meta import ValueMeta
from test_dbnd.tracking.tracking_helpers import get_log_targets


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestTrackingTargets(object):
    def test_path_as_target(self, mock_channel_tracker):
        @task()
        def task_with_log_targets():
            log_target_operation(
                "value", "location://path/to/value.csv", DbndTargetOperationType.read,
            )

        task_with_log_targets()

        log_target_arg = one(get_log_targets(mock_channel_tracker))
        assert log_target_arg.param_name == "value"
        assert log_target_arg.target_path == "location://path/to/value.csv"
        assert log_target_arg.operation_type == DbndTargetOperationType.read
        assert log_target_arg.operation_status == DbndTargetOperationStatus.OK
        assert log_target_arg.value_preview == ""
        assert log_target_arg.data_dimensions is None
        assert log_target_arg.data_schema is None
        assert log_target_arg.data_hash is None

    def test_failed_target(self, mock_channel_tracker):
        @task()
        def task_with_log_targets():
            log_target_operation(
                "value",
                "location://path/to/value.csv",
                DbndTargetOperationType.read,
                operation_status=DbndTargetOperationStatus.NOK,
            )

        task_with_log_targets()

        log_target_arg = one(get_log_targets(mock_channel_tracker))
        assert log_target_arg.param_name == "value"
        assert log_target_arg.target_path == "location://path/to/value.csv"
        assert log_target_arg.operation_type == DbndTargetOperationType.read
        assert log_target_arg.operation_status == DbndTargetOperationStatus.NOK
        assert log_target_arg.value_preview == ""
        assert log_target_arg.data_dimensions is None
        assert log_target_arg.data_schema is None
        assert log_target_arg.data_hash is None

    def test_with_actual_target(self, mock_channel_tracker):
        @task()
        def task_with_log_targets():
            a_target = target("/path/to/value.csv")
            log_target_operation(
                "value", a_target, DbndTargetOperationType.read,
            )

        task_with_log_targets()

        log_target_arg = one(get_log_targets(mock_channel_tracker))
        assert log_target_arg.param_name == "value"
        assert log_target_arg.target_path == "/path/to/value.csv"
        assert log_target_arg.operation_type == DbndTargetOperationType.read
        assert log_target_arg.operation_status == DbndTargetOperationStatus.OK
        assert log_target_arg.value_preview == ""
        assert log_target_arg.data_dimensions is None
        assert log_target_arg.data_schema is None
        assert log_target_arg.data_hash is None

    def test_path_with_target_meta(self, mock_channel_tracker):
        @task()
        def task_with_log_targets():
            log_target_operation(
                "value",
                "/path/to/value.csv",
                DbndTargetOperationType.read,
                target_meta=ValueMeta(
                    value_preview="col_1_val_1\tcol_2_val_1\ncol_1_val_2\tcol_2_val_2",
                    data_dimensions=[2, 2],
                    data_schema={"col_1": "str", "col_2": "str"},
                    data_hash=str(
                        hash("col_1_val_1\tcol_2_val_1\ncol_1_val_2\tcol_2_val_2")
                    ),
                ),
            )

        task_with_log_targets()

        log_target_arg = one(get_log_targets(mock_channel_tracker))

        assert log_target_arg.param_name == "value"
        assert log_target_arg.target_path == "/path/to/value.csv"
        assert log_target_arg.operation_type == DbndTargetOperationType.read
        assert log_target_arg.operation_status == DbndTargetOperationStatus.OK
        assert (
            log_target_arg.value_preview
            == "col_1_val_1\tcol_2_val_1\ncol_1_val_2\tcol_2_val_2"
        )
        assert log_target_arg.data_dimensions == [2, 2]
        assert log_target_arg.data_schema == '{"col_1": "str", "col_2": "str"}'
        assert log_target_arg.data_hash is not None
