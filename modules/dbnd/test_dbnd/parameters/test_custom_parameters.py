import logging

import pandas as pd

from pandas.util.testing import assert_frame_equal
from pytest import fixture

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.custom_parameter_feature_store import (
    CalculateAdvancedFeatures,
    CreateFeatureStoreViaClass,
    FeatureStore,
    report_features,
)
from dbnd_test_scenarios.test_common.custom_parameter_hdf5 import (
    BuildMyData,
    MyDataReport,
)
from targets import target
from targets.target_config import folder
from targets.values.builtins_values import DataValueType
from targets.values.pandas_values import DataFramesDictValueType


logger = logging.getLogger(__name__)


class MyFeatureStoreValueTypeUniversal(DataValueType):
    type = FeatureStore

    def load_from_target(self, target, **kwargs):
        df_dict = DataFramesDictValueType().load_from_target(target, **kwargs)
        return FeatureStore(features=df_dict["/features"], targets=df_dict["/targets"])

    def save_to_target(self, target, value, **kwargs):
        DataFramesDictValueType().save_to_target(
            target=target, value={"features": value.features, "targets": value.targets}
        )


@fixture
def sample_feature_store():
    features = pd.DataFrame(data=[[1, 2], [2, 3]], columns=["Names", "Births"])
    targets = pd.DataFrame(data=[[1, 22], [2, 33]], columns=["Names", "Class"])
    return FeatureStore(features=features, targets=targets)


class TestCustomParameterAndMarshallerParameter(object):
    def test_custom_type_parameter(self):
        t = assert_run_task(BuildMyData())
        assert_run_task(MyDataReport(my_data=t.my_data))

    def test_universal_feature_store_csv(self, tmpdir, sample_feature_store):
        target_dir = str(tmpdir) + "/"
        write_target = target(target_dir, config=folder.csv)
        universal = MyFeatureStoreValueTypeUniversal()
        universal.save_to_target(target=write_target, value=sample_feature_store)

        actual = universal.load_from_target(target=write_target)
        assert_frame_equal(actual.features, sample_feature_store.features)

    def test_universal_feature_store_hdf5(self, tmpdir, sample_feature_store):
        write_target = target(tmpdir, "output.hdf5")
        universal = MyFeatureStoreValueTypeUniversal()
        universal.save_to_target(target=write_target, value=sample_feature_store)

        actual = universal.load_from_target(target=write_target)
        assert_frame_equal(actual.features, sample_feature_store.features)

    def test_feature_store_pipeline(self):
        t = assert_run_task(CreateFeatureStoreViaClass())
        assert_run_task(CalculateAdvancedFeatures(store=t.store))
        assert_run_task(report_features.t(feature_store=t.store))
