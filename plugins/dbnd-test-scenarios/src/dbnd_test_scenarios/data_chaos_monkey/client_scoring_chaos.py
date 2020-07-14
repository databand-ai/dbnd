import datetime

from typing import List

import pandas

from dbnd import Config, parameter


class ClientScoringChaos(Config):
    _conf__task_family = "client_chaos"
    partner = parameter[str]
    fetch_data__column_v2_dates = parameter[List[datetime.datetime]]

    column_v2_dates = parameter[List[datetime.datetime]]


class ClientChaos(object):
    def fetch_data_chaos(self, data: pandas.DataFrame, partner, target_date):
        data.drop
