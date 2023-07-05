# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import dbnd_run_cmd
from dbnd_run.tasks.basics import dbnd_sanity_check
from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


class TestSanityTasks(object):
    def test_sanity_check_task(self):
        check_task = dbnd_sanity_check.task()
        assert_run_task(check_task)

    def test_sanity_check_cmd(self):
        dbnd_run_cmd(["dbnd_sanity_check"])

    # def test_import_package(self):
    #     """Test that all module can be imported"""
    #
    #     project_dir = os.path.join(os.path.dirname(__file__), "..", "src")
    #     good_modules = import_all_modules(
    #         src_dir=project_dir, package="dbnd", excluded=["airflow_operators"]
    #     )
    #
    #     assert len(good_modules) > 20

    def test_import_dbnd_run(self):
        """
        Test that the top databand package can be imported and contains the usual suspects.
        """
        import databand
        import dbnd
        import targets

        from databand import parameters
        from dbnd import tasks

        # These should exist (if not, this will cause AttributeErrors)
        expected = [
            dbnd.Config,
            dbnd.task,
            dbnd.Task,
            dbnd.output,
            dbnd.parameter,
            dbnd.data,
            dbnd.task_namespace,
            databand.task,
            tasks.DataSourceTask,
            tasks.PipelineTask,
            targets.Target,
            targets.DataTarget,
            parameters.Parameter,
            parameters.DateHourParameter,
            parameters.DateMinuteParameter,
            parameters.DateSecondParameter,
            parameters.DateParameter,
            parameters.MonthParameter,
            parameters.YearParameter,
            parameters.DateIntervalParameter,
            parameters.TimeDeltaParameter,
            parameters.IntParameter,
            parameters.FloatParameter,
            parameters.BoolParameter,
        ]
        print(expected)
