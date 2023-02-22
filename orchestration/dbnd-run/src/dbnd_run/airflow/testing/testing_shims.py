# © Copyright Databand.ai, an IBM Company 2022

import logging

import sh

from dbnd_run.airflow.compat import AIRFLOW_VERSION_1, AIRFLOW_VERSION_2


def set_airflow_connection(
    conn_id, conn_type=None, conn_uri=None, extra=None, host=None
):
    airflow_command = ["connections"]

    def _support_v1(arg):
        if AIRFLOW_VERSION_1:
            return arg[0:2] + arg[2:].replace("-", "_")
        return arg

    # name
    airflow_command.extend(
        ["add", conn_id] if AIRFLOW_VERSION_2 else ["--add", "--conn_id", conn_id]
    )
    # conn type
    if conn_type:
        airflow_command.extend([_support_v1("--conn-type"), conn_type])
    if extra:
        airflow_command.extend([_support_v1("--conn-extra"), extra])
    if conn_uri:
        airflow_command.extend([_support_v1("--conn-uri"), conn_uri])
    if host:
        airflow_command.extend([_support_v1("--conn-host"), host])

    try:
        if AIRFLOW_VERSION_2:
            sh.airflow(["connections", "delete", conn_id])
        else:
            sh.airflow(["connections", "--delete", "--conn_id", conn_id])

    except sh.ErrorReturnCode:
        pass

    logging.info("running: airflow {}".format(" ".join(airflow_command)))
    sh.airflow(airflow_command, _truncate_exc=False)
