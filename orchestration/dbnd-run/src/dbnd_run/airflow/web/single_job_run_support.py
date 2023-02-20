# © Copyright Databand.ai, an IBM Company 2022

"""
Support old version of SingleJobRun
Once it has SingleJobRun as polymorfic name
If we have DB with this jobs - we need to make sure SqlAlchemy see this class before we go to
  airflow_url/job/list/

not in use anymore, but legacy DB can contains such objects
"""
from airflow.jobs import BaseJob


class _OldSingleJobRun(BaseJob):
    # if we use real name of the class we need to load it at Airflow Webserver
    __mapper_args__ = {"polymorphic_identity": "SingleDagRunJob"}


def register_legacy_single_job_run():
    """
    make sure sqlalchemy see this class
    """
    return
