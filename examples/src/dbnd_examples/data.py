import datetime
import os

from dbnd import project_path
from dbnd.utils import timezone


def dbnd_examples_data_path(*path):
    non_link_path = os.path.realpath(os.path.dirname(__file__))
    # if env var exists - use it as the examples dir, otherwise, calculate relative from here.
    non_link_path = os.getenv(
        "DBND_EXAMPLES_PATH", os.path.join(non_link_path, "..", "..", "data")
    )
    examples_path = os.path.join(non_link_path, *path)
    return examples_path


class ExamplesData(object):
    vegetables = dbnd_examples_data_path("vegetables_for_greek_salad.txt")
    wines = dbnd_examples_data_path("wine_quality_minimized.csv")
    wines_per_date = dbnd_examples_data_path("wine_quality_per_day")

    wines_full = dbnd_examples_data_path("wine_quality.csv.gz")
    wines_parquet_py27 = dbnd_examples_data_path("wine_quality.py27.parquet")

    raw_logs = timezone.parse("2018-06-22")
    partitioned_data_target_date = datetime.date(year=2018, month=9, day=3)
    partitioned_data = dbnd_examples_data_path("partitioned_data")


data_repo = ExamplesData()


def partner_data_file(*path):
    return project_path("data", "example_raw_data", *path)


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
