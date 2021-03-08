#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging
import os
import subprocess
import sys

from dbnd_airflow.utils import create_airflow_pool


# DO NOT IMPORT ANYTHING FROM AIRFLOW
# we need to initialize some config values first
from dbnd import dbnd_config  # isort:skip
from dbnd._core.context.bootstrap import dbnd_system_bootstrap  # isort:skip


def subprocess_airflow(args):
    """Forward arguments to airflow command line"""

    from airflow.configuration import conf
    from sqlalchemy.engine.url import make_url

    # let's make sure that we user correct connection string
    airflow_sql_conn = conf.get("core", "SQL_ALCHEMY_CONN")
    env = os.environ.copy()
    env["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = airflow_sql_conn
    env["AIRFLOW__CORE__FERNET_KEY"] = conf.get("core", "FERNET_KEY")

    # if we use airflow, we can get airflow from external env
    args = [sys.executable, "-m", "dbnd_airflow"] + args
    logging.info(
        "Running airflow command at subprocess: '%s" " with DB=%s",
        subprocess.list2cmdline(args),
        repr(make_url(airflow_sql_conn)),
    )
    try:
        subprocess.check_call(args=args, env=env)
    except Exception:
        logging.error(
            "Failed to run airflow command %s with path=%s",
            subprocess.list2cmdline(args),
            sys.path,
        )
        raise
    logging.info("Airflow command has been successfully executed")


def subprocess_airflow_initdb():
    logging.info("Initializing Airflow DB")
    return subprocess_airflow(args=["initdb"])


def main(args=None):
    # from dbnd._core.log.config import configure_basic_logging
    # configure_basic_logging(None)

    dbnd_system_bootstrap()

    # LET'S PATCH AIRFLOW FIRST
    from dbnd_airflow.bootstrap import dbnd_airflow_bootstrap

    dbnd_airflow_bootstrap()

    from airflow.bin.cli import CLIFactory
    from airflow.configuration import conf
    from dbnd_airflow.plugins.setup_plugins import (
        setup_scheduled_dags,
        setup_versioned_dags,
    )

    # ORIGINAL CODE from  airflow/bin/airflow
    if conf.get("core", "security") == "kerberos":
        os.environ["KRB5CCNAME"] = conf.get("kerberos", "ccache")
        os.environ["KRB5_KTNAME"] = conf.get("kerberos", "keytab")

    import argcomplete
    from dbnd_airflow.scheduler.dagrun_zombies import find_and_kill_dagrun_zombies

    CLIFactory.subparsers_dict[find_and_kill_dagrun_zombies.__name__] = {
        "func": find_and_kill_dagrun_zombies,
        "help": "Clean up BackfillJob zombie tasks",
        "args": tuple(),
    }

    parser = CLIFactory.get_parser()
    argcomplete.autocomplete(parser)
    args = parser.parse_args(args=args)
    func_name = args.func.__name__

    # DBND PATCH:
    if dbnd_config.getboolean("airflow", "auto_add_scheduled_dags") and func_name in [
        "scheduler",
        "webserver",
    ]:
        setup_scheduled_dags()
    if dbnd_config.getboolean("airflow", "auto_add_versioned_dags") and func_name in [
        "webserver"
    ]:
        setup_versioned_dags()

    args.func(args)

    if func_name in ["resetdb", "initdb"]:
        pool_name = dbnd_config.get("airflow", "dbnd_pool")
        if pool_name == "dbnd_pool":
            create_airflow_pool(pool_name)


if __name__ == "__main__":
    main()
