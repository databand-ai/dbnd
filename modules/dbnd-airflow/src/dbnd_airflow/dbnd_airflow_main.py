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

import argcomplete


# DO NOT IMPORT ANYTHING FROM AIRFLOW
# we need to initialize some config values first


def subprocess_airflow(args):
    """Forward arguments to airflow command line"""
    args = ["airflow"] + args
    logging.info("Running airflow command: %s", subprocess.list2cmdline(args))
    subprocess.check_call(args=args)

    logging.info("Airflow command has been successfully executed")


def main(args=None):
    # from dbnd._core.log.config import configure_basic_logging
    # configure_basic_logging(None)

    from dbnd import dbnd_config
    from dbnd._core.configuration.environ_config import set_quiet_mode
    from dbnd._core.context.bootstrap import dbnd_system_bootstrap

    set_quiet_mode()
    dbnd_system_bootstrap()

    # LET'S PATCH AIRFLOW FIRST
    from dbnd_airflow.bootstrap import airflow_bootstrap

    airflow_bootstrap()

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


if __name__ == "__main__":
    main()
