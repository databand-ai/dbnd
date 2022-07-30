# © Copyright Databand.ai, an IBM Company 2022

import datetime
import os

from dbnd._core.utils.project.project_fs import abs_join
from dbnd.utils import timezone


def _dbnd_examples_project_root():
    # if env var exists - use it as the examples dir, otherwise, calculate relative from here.
    dbnd_examples_project = os.getenv("DBND_EXAMPLES_PATH", None)
    if dbnd_examples_project:
        return dbnd_examples_project

    return abs_join(__file__, "..", "..", "..")


def dbnd_examples_project_path(*path):
    return os.path.join(_dbnd_examples_project_root(), *path)


def dbnd_examples_src_path(*path):
    return dbnd_examples_project_path("src", "dbnd_examples", *path)


def dbnd_examples_data_path(*path):
    return dbnd_examples_project_path("data", *path)


class ExamplesData(object):
    vegetables = dbnd_examples_data_path("vegetables_for_greek_salad.txt")
    wines = dbnd_examples_data_path("wine_quality_minimized.csv")
    wines_per_date = dbnd_examples_data_path("wine_quality_per_day")

    wines_full = dbnd_examples_data_path("wine_quality.csv.gz")

    raw_logs = timezone.parse("2018-06-22")
    partitioned_data_target_date = datetime.date(year=2018, month=9, day=3)
    partitioned_data = dbnd_examples_data_path("partitioned_data")


data_repo = ExamplesData()


def partner_data_file(*path):
    return dbnd_examples_data_path("example_raw_data", *path)


class PartnerData(object):
    seed = dbnd_examples_data_path("pipeline_train_seed_data.csv")

    def partner_a_file(self, date):
        return partner_data_file("partner_a", date.strftime("%Y-%m-%d") + ".csv")

    def partner_b_file(self, date):
        return partner_data_file("partner_b", date.strftime("%Y-%m-%d") + ".json")

    def partner_c_file(self, date):
        return partner_data_file("partner_c", date.strftime("%Y-%m-%d") + ".csv")

    def partner_to_file(self, name, date):
        return {
            "a": self.partner_a_file(date),
            "b": self.partner_b_file(date),
            "c": self.partner_c_file(date),
        }[name]

    def sample_customer_file(self):
        return partner_data_file("customer_a.csv")


demo_data_repo = PartnerData()
