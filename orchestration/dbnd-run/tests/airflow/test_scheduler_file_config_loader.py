# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest
import six

from airflow import DAG

from dbnd import dbnd_config
from dbnd_run.airflow.scheduler.dags_provider_from_file import (
    DbndAirflowDagsProviderFromFile,
    InvalidConfigException,
    get_dags_from_file,
)


logger = logging.getLogger(__name__)


class TestSchedulerFileConfigLoader(object):
    @pytest.fixture
    def tmp_config_file(self, tmpdir):
        tmp_config_file = str(tmpdir.join("test_config_file.yaml"))
        with dbnd_config({"scheduler": {"config_file": tmp_config_file}}):
            yield tmp_config_file

    def test_all_commands_scheduler(self, tmp_config_file):
        with open(tmp_config_file, "w") as fp:
            fp.write(
                """
        - name: Active out of Monitored Leads
          cmd: dbnd run dbnd_sanity_check --set task_target_date={{tomorrow_ds}} --task-version now
          schedule_interval: "1 3-19 * * *"
          start_date: 2021-02-15T00:00:00
          catchup: false
          active: true
        """
            )

        dags = get_dags_from_file()
        for dag_id, dag in six.iteritems(dags):
            with dag:
                cmd = dag.tasks[0].scheduled_cmd
            assert cmd.startswith("dbnd run")
            assert isinstance(dag, DAG)

    def test_simple_file(self, tmp_config_file):
        with open(tmp_config_file, "w") as fp:
            fp.write(
                """
        - name: dbnd_sanity_check
          cmd: dbnd run dbnd_sanity_check --task-version now
          schedule_interval: "* * * * *"
          start_date: 2021-02-15T00:00:00
          retries: 2
          active: true

        """
            )
        jobs = DbndAirflowDagsProviderFromFile(
            config_file=tmp_config_file
        ).read_config()

        assert len(jobs) == 1
        assert jobs[0]["name"] == "dbnd_sanity_check"
        assert jobs[0]["cmd"] == "dbnd run dbnd_sanity_check --task-version now"
        assert jobs[0]["schedule_interval"] == "* * * * *"
        assert jobs[0]["retries"] == 2
        assert jobs[0]["active"] is True

    def test_broken_yaml_1(self, tmp_config_file):
        with pytest.raises(InvalidConfigException):
            with open(tmp_config_file, "w") as fp:
                fp.write("just a string")
            get_dags_from_file()

    def test_broken_yaml_2(self, tmp_config_file):
        with pytest.raises(InvalidConfigException):
            with open(tmp_config_file, "w") as fp:
                fp.write("@@@")
            get_dags_from_file()

    def test_validation_errors(self, tmp_config_file):
        with open(tmp_config_file, "w") as fp:
            fp.write(
                """
        - name: dbnd_sanity_check
          schedule_interval: "@ * * * *"
          retries: five
          active: BANANA

        - name: dbnd_sanity_check

        - retries: 1
        """
            )
        try:
            get_dags_from_file()
            assert False
        except InvalidConfigException as ex:
            assert ex
            msg = str(ex)
            assert "active: Not a valid boolean" in msg
            assert "cmd: Missing data for required field" in msg
            assert "retries: Not a valid integer" in msg
            assert "Invalid schedule_interval: [@ * * * *] is not acceptable" in msg
            assert (
                "1 other entry exist in the configuration file with the same name"
                in msg
            )
