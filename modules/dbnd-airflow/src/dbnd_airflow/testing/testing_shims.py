# © Copyright Databand.ai, an IBM Company 2022

import logging

import sh

from dbnd_airflow.constants import AIRFLOW_VERSION_2


def build_airflow_conn_command(name, conn_type, extra=None, host=None):
    airflow_command = ["connections"]

    # name
    airflow_command.extend(
        ["add", name] if AIRFLOW_VERSION_2 else ["--add", "--conn_id", name]
    )
    # conn type
    airflow_command.extend(
        ["--conn-type", conn_type] if AIRFLOW_VERSION_2 else ["--conn_type", conn_type]
    )
    # conn extra
    if extra:
        airflow_command.extend(
            ["--conn-extra", extra] if AIRFLOW_VERSION_2 else ["--conn_extra", extra]
        )
    # conn host
    if host:
        airflow_command.extend(
            ["--conn-host", host] if AIRFLOW_VERSION_2 else ["--conn_host", host]
        )

    return airflow_command


def set_airflow_connection(dbnd_conn_name, conn_type, extra=None, host=None):
    airflow_command = build_airflow_conn_command(
        name=dbnd_conn_name, conn_type=conn_type, extra=extra, host=host
    )
    try:
        if AIRFLOW_VERSION_2:
            sh.airflow(["connections", "delete", dbnd_conn_name])
        else:
            sh.airflow(["connections", "--delete", "--conn_id", dbnd_conn_name])

    except sh.ErrorReturnCode:
        pass

    logging.info("running: airflow {}".format(" ".join(airflow_command)))
    sh.airflow(airflow_command, _truncate_exc=False)


def run_dag_backfill(dag_id, backfill_date):
    if AIRFLOW_VERSION_2:
        airflow_command = f"dags backfill -s {backfill_date} -e {backfill_date} {dag_id} --reset-dagruns --yes"
    else:
        airflow_command = f"backfill -s {backfill_date} -e {backfill_date} {dag_id} --reset_dagruns --yes"

    logging.info("running: airflow {}".format(airflow_command))
    airflow_process = sh.airflow(airflow_command.split(), _bg=True, _truncate_exc=False)
    airflow_process.wait()
