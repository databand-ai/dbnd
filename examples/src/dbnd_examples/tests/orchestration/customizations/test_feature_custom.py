# © Copyright Databand.ai, an IBM Company 2022

from pandas import DataFrame

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.customizations.custom_output_factory import (
    DataSplitIntoMultipleOutputs,
)
from dbnd_examples.orchestration.customizations.custom_output_location import (
    GenerateReportToCustomLocation,
)
from dbnd_examples.orchestration.customizations.custom_value_type import (
    MyCustomObject,
    TaskWithCustomValue,
    TaskWithCustomValueInline,
)
from dbnd_test_scenarios.test_common.custom_parameter_feature_store import (
    calculate_features,
    calculate_features_via_classes,
)
from dbnd_test_scenarios.test_common.custom_parameter_hdf5 import MyHdf5DataPipeline


class TestFeatureCustom(object):
    def test_data_split_into_multiple_outputs(self):
        task = assert_run_task(DataSplitIntoMultipleOutputs())
        assert "part_0" in task.splits
        assert "train_part_0" in str(task.splits["part_0"][0])

    def test_generate_report_to_different_location(self):
        task = assert_run_task(GenerateReportToCustomLocation(name="my_report"))
        assert "/my_report/" in str(task.report)

    def test_feature_store_via_func(self):
        task = assert_run_task(calculate_features.task(ratio=10))
        assert task.validation.load(str) == "OK"

    def test_feature_store_via_classes(self):
        task = assert_run_task(calculate_features_via_classes.task(ratio=10))
        assert not task.result.load(DataFrame).empty

    def test_my_data_value_type(self):
        task = assert_run_task(MyHdf5DataPipeline())
        assert not task.report.load(DataFrame).empty

    def test_custom_object_simple(self):
        t = assert_run_task(
            TaskWithCustomValue(custom_value="1", list_of_customs="['1','2']")
        )
        assert "12" == t.report.load(str)

    def test_custom_object_inline(self):
        t = assert_run_task(
            TaskWithCustomValueInline(
                custom_value=MyCustomObject("cv"),
                custom_value_with_default="cvd",
                list_of_customs=[MyCustomObject("l1"), MyCustomObject("l2")],
                set_of_customs="1,2",
                dict_of_customs="{'a':'from_dict'}",
            )
        )
        assert "cv_cvd_l1_from_dict" == t.report.load(str)

    def test_custom_config(self):
        from dbnd_examples.orchestration.customizations.custom_paramester_user_object import (
            config_flow,
        )

        assert_run_task(config_flow.task(my_config="1"))
