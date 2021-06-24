import logging

from typing import Dict, List

from pandas import DataFrame

from dbnd import band, log_dataframe, parameter, task
from dbnd.utils import data_combine
from targets import Target


logger = logging.getLogger(__name__)


@task
def dump_table_from_db(table):
    # type: (str) -> List[str]
    return ["raw_id,partner,value\n"] + [
        "%s_%s,%s,%s\n" % (table, idx, idx % 3, idx * 2) for idx in range(10)
    ]


@task
def filter_by_id(data, partner_id):
    # type: (DataFrame, int) -> DataFrame
    partner_data = data[data["partner"] == partner_id]
    log_dataframe("partner_%s" % partner_id, partner_data)
    return partner_data


@task(partners_files=parameter.data)
def build_report_one_by_one(partners_files):
    # type: (Dict[int,Target]) -> str
    logger.info("partners files: %s", partners_files)
    return "OK"


@task
def build_report_from_dataframe(partners_df):
    # type: (DataFrame) -> str
    logger.info("partners data: %s", partners_df)
    return str(len(partners_df))


@band
def output_per_id_report(partners):
    # type: (List[int]) -> ...
    dump = dump_table_from_db(table="my_table")
    partners_info = {partner: filter_by_id(dump, partner) for partner in partners}

    report_one_by_one = build_report_one_by_one(partners_files=partners_info)

    report_from_dataframe = build_report_from_dataframe(
        partners_df=data_combine(partners_info.values())
    )
    return report_one_by_one, report_from_dataframe
