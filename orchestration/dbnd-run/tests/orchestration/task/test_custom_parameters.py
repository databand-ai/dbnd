# © Copyright Databand.ai, an IBM Company 2022

import logging

import pandas as pd

from pandas.util.testing import assert_frame_equal
from pytest import fixture

from dbnd_run.testing.helpers import assert_run_task
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
from targets.caching import TARGET_CACHE
from targets.target_config import folder
from targets.values import get_value_type_of_type
from targets.values.builtins_values import DataValueType


logger = logging.getLogger(__name__)


class MyFeatureStoreValueTypeUniversal(DataValueType):
    type = FeatureStore

    def load_from_target(self, target, **kwargs):
        value_type = get_value_type_of_type("Dict[str, pd.DataFrame]").load_value_type()
        df_dict = value_type.load_from_target(target, **kwargs)
        logger.info(df_dict)
        # if from target cache  - no "/" in key
        return FeatureStore(features=df_dict["/features"], targets=df_dict["/targets"])

    def save_to_target(self, target, value, **kwargs):
        value_type = get_value_type_of_type("Dict[str, pd.DataFrame]").load_value_type()
        value_type.save_to_target(
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
        TARGET_CACHE.clear()
        actual = universal.load_from_target(target=write_target)
        assert_frame_equal(actual.features, sample_feature_store.features)

    def test_universal_feature_store_hdf5(self, tmpdir, sample_feature_store):
        write_target = target(tmpdir, "output.hdf5")
        universal = MyFeatureStoreValueTypeUniversal()
        universal.save_to_target(target=write_target, value=sample_feature_store)

        TARGET_CACHE.clear()
        actual = universal.load_from_target(target=write_target)
        assert_frame_equal(actual.features, sample_feature_store.features)

    def test_feature_store_pipeline(self):
        t = assert_run_task(CreateFeatureStoreViaClass())
        assert_run_task(CalculateAdvancedFeatures(store=t.store))
        assert_run_task(report_features.t(feature_store=t.store))
