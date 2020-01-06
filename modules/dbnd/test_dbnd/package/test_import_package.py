# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Modifications copyright (C) 2018 databand.ai
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
import os

import dbnd._core.task_build.task_namespace

from dbnd._core.utils.project.project_fs import project_path


logger = logging.getLogger(__name__)


class TestImportPackage(object):
    def test_import_package(self):
        """Test that all module can be imported
        """

        project_dir = os.path.join(os.path.dirname(__file__), "..", "..", "src")
        packagedir = os.path.join(project_dir, "dbnd")

        errors = []
        good_modules = []

        def import_module(p):
            try:
                logger.info("Importing %s", p)
                __import__(p)
                good_modules.append(p)
            except Exception as ex:
                errors.append(ex)
                logger.exception("Failed to import %s", p)

        excluded = ["airflow_operators"]

        for root, subdirs, files in os.walk(packagedir):
            package = os.path.relpath(root, start=project_dir).replace(os.path.sep, ".")

            if any([p in root for p in excluded]):
                continue

            if "__init__.py" not in files:
                continue

            import_module(package)

            for f in files:
                if f.endswith(".py") and not f.startswith("_"):
                    import_module(package + "." + f[:-3])

        assert not errors
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
