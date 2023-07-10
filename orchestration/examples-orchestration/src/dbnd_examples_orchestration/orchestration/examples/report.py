# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, List

import matplotlib
import pandas as pd

from matplotlib.figure import Figure
from pandas import DataFrame, np

from dbnd import band, log_dataframe, output, task
from dbnd.utils import data_combine
from dbnd_run.tasks.basics.publish import publish_results
from targets import Target


matplotlib.use("Agg")
logger = logging.getLogger(__name__)


@task
def dump_table_from_db(table, db_name="master"):
    # type: (str, str) -> List[str]
    # output: raw_id(table+id), partner, value
    return ["raw_id,partner,value\n"] + [
        "%s_%s,%s,%s\n" % (table, idx, idx % 3, idx * 2) for idx in range(10)
    ]


@band
def dump_db(tables, db_name="master"):
    # type: (List[str], str) -> Dict[str, pd.DataFrame]
    result = {
        t: dump_table_from_db(table=t, db_name=db_name, task_name="table_%s" % t)
        for t in tables
    }
    return result


@task
def filter_partner(data, partner):
    # type: (DataFrame, int) -> DataFrame
    partner_data = data[data["partner"] == partner]
    log_dataframe("partner_%s" % partner, partner_data)
    return partner_data


@task(result=output.folder[Dict[str, DataFrame]])
def separate_data_by_type(data):
    # type: (DataFrame) -> Dict[str, DataFrame]

    return {"a": data.head(1), "b": data.head(2), "c": data.head(1)}


@task
def build_report(partners_files, partners_df):
    # type: (Dict[int,Target], DataFrame) -> str
    logger.info("partners files: %s", partners_files)
    logger.info("partners data: %s", partners_df)
    return "OK"


@task
def build_graphs(partner_info):
    # type: (DataFrame) -> Figure
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax1 = fig.add_subplot(2, 2, 1)
    ax1.hist(np.random.randn(100), bins=20, alpha=0.3)
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.scatter(np.arange(30), np.arange(30) + 3 * np.random.randn(30))
    fig.add_subplot(2, 2, 3)
    return fig


@band
def partners_report(partners):
    # type: (List[int]) -> ...
    table = "aa"

    dump = dump_db(tables=[table])
    partners_info = {
        partner: filter_partner(dump[table], partner) for partner in partners
    }
    by_type = separate_data_by_type(dump[table])

    report = build_report(
        partners_files=partners_info, partners_df=data_combine(partners_info.values())
    )

    graphs = {partner: build_graphs(pi) for partner, pi in partners_info.items()}

    published = publish_results(graphs).published

    return report, by_type, published
