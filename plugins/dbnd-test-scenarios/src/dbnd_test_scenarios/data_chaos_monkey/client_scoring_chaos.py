import datetime


COLUMN_10_DATE = datetime.date(2020, 7, 11)


# class ClientScoringChaos(Config):
#     _conf__task_family = "client_chaos"
#     partner = parameter[str]
#     fetch_data__column_v2_dates = parameter[List[datetime.datetime]]
#
#     column_v2_dates = parameter[List[datetime.datetime]]
#


def is_chaos_column_10(partner, task_target_date):
    if partner in ["autolab", "autobig", "myp"] and task_target_date >= COLUMN_10_DATE:
        return True
    return False


def chaos_model_metric(value, task_target_date):
    if task_target_date and task_target_date >= COLUMN_10_DATE:
        return value / 10
    return value
