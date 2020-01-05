import logging

import dbnd._core.task_build.task_namespace

from dbnd._core.utils.project.project_fs import project_path
from dbnd.testing.helpers import import_all_modules


logger = logging.getLogger(__name__)


class TestImportPackage(object):
    def test_import_package(self):
        """Test that all module can be imported
        """

        project_dir = project_path("modules", "dbnd", "src")
        good_modules = import_all_modules(
            src_dir=project_dir, package="dbnd", excluded=["airflow_operators"]
        )

        assert len(good_modules) > 20

    def test_import_databand(self):
        """
        Test that the top databand package can be imported and contains the usual suspects.
        """
        import dbnd
        import databand
        from databand import parameters
        from dbnd import tasks
        import targets

        # These should exist (if not, this will cause AttributeErrors)
        expected = [
            dbnd.Config,
            dbnd.Task,
            dbnd.output,
            dbnd.parameter,
            dbnd.data,
            databand.task,
            tasks.DataSourceTask,
            tasks.PipelineTask,
            targets.Target,
            targets.DataTarget,
            dbnd._core.task_build.task_namespace.namespace,
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
        assert len(expected) > 0
