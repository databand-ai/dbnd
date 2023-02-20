# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from dbnd._core.utils.project.project_fs import abs_join, relative_path
from dbnd_test_scenarios.utils.data_chaos_monkey.client_scoring_chaos import (
    is_chaos_column_10,
)
from targets import target


logger = logging.getLogger(__name__)
_PLUGIN_ROOT = relative_path(__file__, "..", "..")
_PLUGIN_SRC_ROOT = relative_path(__file__)


def scenario_root_path(*path):
    return abs_join(_PLUGIN_ROOT, *path)


def scenario_src_path(*path):
    return abs_join(_PLUGIN_SRC_ROOT, *path)


def test_scenario_path(*path):
    return scenario_root_path("scenarios", *path)


def test_scenario_target(*path):
    return target(test_scenario_path(*path))


def scenario_data_path(*path):
    return scenario_root_path("data", *path)


def scenario_data_target(*path):
    return target(scenario_data_path(*path))


def scenario_pyspark_path(*path):
    return scenario_src_path("spark", "pyspark_scripts", *path)


class _Scenarios(object):
    pass


class _ScenariosClientScoringData(object):
    p_g_ingest_data = scenario_data_target("client_scoring/p_g_ready_for_ingest.csv")
    p_g_ingest_data__no_col_10 = scenario_data_target(
        "client_scoring/p_g_ready_for_ingest__no_col_10.csv"
    )

    p_g_train_data = scenario_data_target("client_scoring/p_g_ready_for_train.csv")

    p_a_master_data_bad = scenario_data_target("client_scoring/p_a_master_data_bad.csv")

    partners = ["autolab", "picsdata", "myp"]
    partners_big = ["autobig", "picsbig"]

    def get_ingest_data(self, partner, target_date_str):
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        if is_chaos_column_10(partner, target_date):
            return self.p_g_ingest_data__no_col_10
        return self.p_g_ingest_data


scenarios = _Scenarios()
client_scoring_data = _ScenariosClientScoringData()
