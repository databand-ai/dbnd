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

import copy
import os
import sys

from airflow import configuration as conf
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from pytest import fixture, mark

from dbnd._core.utils.date_utils import airflow_datetime_str
from dbnd_test_scenarios.test_common.task.factories import TTask
from test_dbnd_airflow.utils import WebAppTest


@fixture
def logging_config_for_log_view(tmpdir):
    # Create a custom logging configuration
    logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logging_config["handlers"]["task"]["base_log_folder"] = os.path.normpath(
        os.path.join(current_dir, "test_logs")
    )
    logging_config["handlers"]["task"]["filename_template"] = (
        "{{ ti.dag_id }}/{{ ti.task_id }}/"
        '{{ ts | replace(":", ".") }}/{{ try_number }}.log'
    )

    # Write the custom logging configuration to a file
    settings_folder = str(tmpdir)
    settings_file = os.path.join(settings_folder, "airflow_local_settings.py")
    new_logging_file = "LOGGING_CONFIG = {}".format(logging_config)
    with open(settings_file, "w") as handle:
        handle.writelines(new_logging_file)
    sys.path.append(settings_folder)
    conf.set("core", "logging_config_class", "airflow_local_settings.LOGGING_CONFIG")

    yield
    sys.path.remove(settings_folder)
    conf.set("core", "logging_config_class", "")


class TestLogView(WebAppTest):
    @fixture(autouse=True)
    def _test_run(self, databand_test_context, logging_config_for_log_view):
        task = TTask()
        run = task.dbnd_run()

        self.task = task
        self.task_af_id = run.get_task_run_by_id(task.task_id).task_af_id
        self.dag_id = run.dag_id
        self.task_execution_date_str = airflow_datetime_str(run.execution_date)

    def _url(self, endpoint, **kwargs):
        new_kwargs = kwargs.copy()

        new_kwargs.setdefault("dag_id", self.dag_id)
        new_kwargs["execution_date"] = self.task_execution_date_str
        new_kwargs["task_id"] = self.task_af_id

        return super(TestLogView, self)._url(endpoint, **new_kwargs)

    def test_get_file_task_log(self):
        self.assert_view("Airflow.log", "Log by attempts")

    @mark.skip
    def test_get_logs_with_metadata(self):
        expected = ['"message":', '"metadata":', "has been completed"]
        self.assert_view(
            "Airflow.get_logs_with_metadata", expected, try_number=1, metadata={}
        )
