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
import json
import logging

import airflow

from airflow.utils import timezone
from pkg_resources import parse_version
from pytest import fixture

from dbnd._core.utils.date_utils import airflow_datetime_str
from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import patch_airflow_create_app
from dbnd_test_scenarios.test_common.task.factories import TTask
from test_dbnd_airflow.web.utils import (
    WebAppTest,
    assert_content_in_response,
    assert_ok,
)


logger = logging.getLogger(__name__)


class TestAirflowBaseViews(WebAppTest):
    task_execution_date_str = timezone.datetime(2018, 3, 1)

    @fixture(autouse=True)
    def _test_run(self, databand_test_context):
        patch_airflow_create_app()

    @fixture(autouse=True)
    def _test_run(self, databand_test_context):
        task = TTask()
        run = task.dbnd_run()

        self.task = task
        self.task_af_id = run.get_task_run_by_id(task.task_id).task_af_id
        self.dag_id = run.dag_id
        self.task_execution_date_str = airflow_datetime_str(run.execution_date)

    def _set_defaults(self, **kwargs):
        new_kwargs = kwargs.copy()

        if not kwargs.pop("no_dag_id", None):
            new_kwargs.setdefault("dag_id", self.dag_id)
        if kwargs.pop("with_execution_date", None):
            new_kwargs["execution_date"] = self.task_execution_date_str

        return new_kwargs

    def _url(self, endpoint, **kwargs):
        new_kwargs = self._set_defaults(**kwargs)

        return super(TestAirflowBaseViews, self)._url(endpoint, **new_kwargs)

    def test_index(self):
        resp = self.client.get("/", follow_redirects=True)
        assert_content_in_response("DAGs", resp)

    def test_health(self):
        # case-3: unhealthy scheduler status - scenario 2 (no running SchedulerJob)
        actual = self.client.get("health", follow_redirects=True).data
        assert actual
        resp_json = json.loads(actual.decode("utf-8"))

        assert "healthy" == resp_json["metadatabase"]["status"]

    def test_home(self):
        resp = self.client.get("home", follow_redirects=True)
        assert_content_in_response("DAGs", resp)

    def test_sub_task_details(self):
        self.assert_view(
            "Airflow.task",
            "Task Instance Details",
            with_execution_date=True,
            task_id=self.task_af_id,
        )

    def test_xcom(self):
        self.assert_view(
            "Airflow.xcom", "XCom", with_execution_date=True, task_id=self.task_af_id
        )

    def test_rendered(self):
        self.assert_view(
            "Airflow.rendered",
            "Rendered Template",
            with_execution_date=True,
            task_id=self.task_af_id,
        )

    #
    # def test_pickle_info(self):
    #     self.assert_view("Airflow.pickle_info", "info", task_id=self.task_af_id)

    def test_dag_details(self):
        self.assert_view("Airflow.dag_details", "DAG details")

    def test_graph(self):
        self.assert_view("Airflow.graph", self.task.get_task_family())

    def test_tree(self):
        self.assert_view("Airflow.tree", self.task.get_task_family())

    def test_duration(self):
        self.assert_view("Airflow.duration", self.task.get_task_family(), days=30)

    def test_tries(self):
        self.assert_view("Airflow.tries", self.task.get_task_family(), days=30)

    def test_landing_times(self):
        self.assert_view("Airflow.landing_times", self.task.get_task_family(), days=30)

    def test_gantt(self):
        self.assert_view("Airflow.gantt", self.task.get_task_family(), days=30)

    def test_code(self):
        self.assert_view("Airflow.code", [], days=30)

    def test_blocked(self):
        url = "blocked"
        if parse_version(airflow.version.version) >= parse_version("1.10.10"):
            resp = self.client.post(url, follow_redirects=True)
        else:
            resp = self.client.get(url, follow_redirects=True)
        assert_ok(resp)

    def test_dag_stats(self):
        if parse_version(airflow.version.version) >= parse_version("1.10.10"):
            resp = self.client.post("dag_stats", follow_redirects=True)
        else:
            resp = self.client.get("dag_stats", follow_redirects=True)
        assert_ok(resp)

    def test_task_stats(self):
        if parse_version(airflow.version.version) >= parse_version("1.10.10"):
            resp = self.client.post("task_stats", follow_redirects=True)
        else:
            resp = self.client.get("task_stats", follow_redirects=True)
        assert_ok(resp)

    def test_paused(self):
        url = self._url("Airflow.paused", is_paused=False)
        resp = self.client.post(url, follow_redirects=True)
        assert_content_in_response("OK", resp)

    def test_success(self):
        url = self._url("Airflow.success")
        data = self._set_defaults(
            task_id=self.task_af_id,
            with_execution_date=True,
            upstream=False,
            downstream=False,
            future=False,
            past=False,
        )
        resp = self.client.post(url, data=data)
        assert_content_in_response("Wait a minute", resp)

    def test_clear(self):
        url = self._url("Airflow.clear")
        data = self._set_defaults(
            task_id=self.task_af_id,
            with_execution_date=True,
            upstream=False,
            downstream=False,
            future=False,
            past=False,
        )
        resp = self.client.post(url, data=data)
        assert_content_in_response(["TTask", "Wait a minute"], resp)

    def test_run(self):
        url = self._url("Airflow.run")
        data = self._set_defaults(
            task_id=self.task_af_id,
            with_execution_date=True,
            ignore_all_deps=False,
            ignore_ti_state=True,
        )
        resp = self.client.post(url, data=data)
        assert_content_in_response("", resp, resp_code=302)

    def test_refresh(self):
        resp = self.client.post(self._url("Airflow.refresh"))
        assert_content_in_response("", resp, resp_code=302)
