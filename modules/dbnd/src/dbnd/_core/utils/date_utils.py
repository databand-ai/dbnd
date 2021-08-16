from datetime import timedelta

from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


def airflow_datetime_str(datetime):
    return pendulum.instance(datetime).to_iso8601_string(extended=True)


_current_unique_execution_date_salt = 0


def unique_execution_date():
    """
    create unique exec date for every run ( so it runs! )
    it should not success datetime.now()
    """
    global _current_unique_execution_date_salt
    _current_unique_execution_date_salt += 1
    if _current_unique_execution_date_salt == 1:
        return utcnow()  # just return current utc

    # task can't run ahead of time! deadlock in scheduler so we go back 1 sec
    # and add unique salt - every time we have new execution date
    return (
        utcnow()
        - timedelta(seconds=1)
        + timedelta(microseconds=_current_unique_execution_date_salt)
    )


def get_isoformat_date(date):
    """
    Returns parsed isofromat date
    :param date: Optional[datetime]

    :returns Optional[datetime]
    """
    from dbnd._core.utils.basics.nothing import NOTHING

    return date.isoformat() if date not in (NOTHING, None) else date
