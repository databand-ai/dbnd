import os
import shutil
import sys

import mock
import pytest

from dbnd_luigi.luigi_task import wrap_luigi_task
from dbnd_luigi.luigi_tracking import dbnd_luigi_build, dbnd_luigi_run
from luigi import LuigiStatusCode
from tests.conftest import delete_task_output
from tests.luigi_examples.top_artists import LuigiTestException, MyPostgresQuery

from dbnd import dbnd_config
from dbnd._core.parameter.parameter_definition import _ParameterKind
from dbnd._core.settings import CoreConfig


class TestLuigiTaskExecution(object):
    @pytest.fixture(autouse=True)
    def clean_output(self):
        import os

        os.system("rm -rf ./data/*")

    def test_luigi_sanity_top_10_artists(self, top10_artists):
        with dbnd_config({CoreConfig.databand_url: "http://localhost:8080"}):
            with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
                result = dbnd_luigi_build(tasks=[top10_artists])
        assert result.status == LuigiStatusCode.SUCCESS

    def test_luigi_sanity_foo(self, simple_foo):
        with dbnd_config({CoreConfig.databand_url: "http://localhost:8080"}):
            with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
                try:
                    shutil.rmtree("/tmp/bar")
                except FileNotFoundError:
                    pass
            result = dbnd_luigi_build(tasks=[simple_foo])
        assert result.status == LuigiStatusCode.SUCCESS

    @pytest.mark.skip
    def test_luigi_sanity_complex_foo(self, complex_foo):
        with dbnd_config({CoreConfig.databand_url: "http://localhost:8080"}):
            with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
                try:
                    shutil.rmtree("/tmp/bar")
                except FileNotFoundError:
                    pass
                result = dbnd_luigi_build(tasks=[complex_foo])
        assert result.status == LuigiStatusCode.SUCCESS

    def test_luigi_orphan_task(self, streams):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            result = dbnd_luigi_build(tasks=[streams])
        assert result.status == LuigiStatusCode.SUCCESS

    def test_luigi_run_exception(self):
        sys.argv = [
            "luigi",
            "Top10ArtistsRunException",
            "--Top10ArtistsRunException-date-interval",
            "2020-05-02",
            "--local-scheduler",
            "--module",
            str("tests.luigi_examples.top_artists"),
        ]
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with mock.patch("dbnd_luigi.luigi_tracking.handler") as handler:
                result = dbnd_luigi_run()
                assert handler.on_failure.call_count == 1
                assert handler.on_success.call_count == 2
                assert handler.on_dependency_discovered.call_count == 2
                assert handler.on_run_start.call_count == 3
                assert result.status == LuigiStatusCode.FAILED

    def test_luigi_build_exception(self, top10_artists_run_error):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with mock.patch("dbnd_luigi.luigi_tracking.handler") as handler:
                result = dbnd_luigi_build(tasks=[top10_artists_run_error])
                assert handler.on_failure.call_count == 1
                assert handler.on_success.call_count == 3
                assert handler.on_dependency_discovered.call_count == 3
                assert handler.on_run_start.call_count == 4
                assert result.status == LuigiStatusCode.FAILED

    def test_luigi_requires_exception(self, top10_artists_requires_error):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with pytest.raises(LuigiTestException):
                result = dbnd_luigi_build(tasks=[top10_artists_requires_error])
                assert result.status == LuigiStatusCode.SCHEDULING_FAILED

    def test_luigi_output_exception(self, top10_artists_output_error):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with pytest.raises(LuigiTestException):
                result = dbnd_luigi_build(tasks=[top10_artists_output_error])
                assert result.status == LuigiStatusCode.SCHEDULING_FAILED


class TestLuigiWrapperTaskExecution(object):
    def test_luigi_wrapper_task_sanity(self, wrapper_task):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            result = dbnd_luigi_build(tasks=[wrapper_task])
        assert result.status == LuigiStatusCode.SUCCESS

    def test_luigi_wrapper_task_run_fail(self, wrapper_task_run_fail):
        delete_task_output(wrapper_task_run_fail)
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            with mock.patch("dbnd_luigi.luigi_tracking.handler") as handler:
                result = dbnd_luigi_build(tasks=[wrapper_task_run_fail])
                assert handler.on_failure.call_count == 1
                assert handler.on_success.call_count == 1
                assert handler.on_dependency_discovered.call_count == 1
                assert handler.on_run_start.call_count == 2
                assert result.status == LuigiStatusCode.FAILED


class TestLuigiWiring(object):
    def test_luigi_sanity_output_target_tracking(self, top10_artists):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            dbnd_task = wrap_luigi_task(top10_artists)
            assert dbnd_task
            assert dbnd_task.task_outputs
            # 'result' is our added output target
            assert len(dbnd_task.task_outputs) == 2
            dbnd_output = [
                v for k, v in dbnd_task.task_outputs.items() if k != "task_band"
            ][0]
            assert dbnd_output
            luigi_output = top10_artists.output()
            assert luigi_output
            # Assert we preserve filename and directory tree format
            assert luigi_output.path in dbnd_output.path

    def test_luigi_sanity_input_target_tracking(self, top10_artists):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            dbnd_task = wrap_luigi_task(top10_artists)
            assert dbnd_task
            dbnd_input_target = [
                x for x in dbnd_task.task_meta.task_params if "artist_streams" in x.name
            ][0].value
            assert dbnd_input_target
            luigi_target = top10_artists.input()
            assert luigi_target
            assert luigi_target.path in dbnd_input_target.path

    def test_multiple_input_tracking(self, task_c):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            dbnd_task = wrap_luigi_task(task_c)
            assert dbnd_task
            # Output1 and 2 are actually inputs from TaskB, just badly named
            assert dbnd_task.output1
            assert dbnd_task.output10
            assert dbnd_task.output2
            assert dbnd_task.output20
            output1 = [
                x for x in dbnd_task.task_meta.task_params if x.name == "output1"
            ][0]
            output10 = [
                x for x in dbnd_task.task_meta.task_params if x.name == "output10"
            ][0]
            output2 = [
                x for x in dbnd_task.task_meta.task_params if x.name == "output2"
            ][0]
            output20 = [
                x for x in dbnd_task.task_meta.task_params if x.name == "output20"
            ][0]
            assert output1.parameter.kind == _ParameterKind.task_input
            assert output10.parameter.kind == _ParameterKind.task_input
            assert output2.parameter.kind == _ParameterKind.task_input
            assert output20.parameter.kind == _ParameterKind.task_input

    def test_multiple_output_tracking(self, task_b):
        with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
            dbnd_task = wrap_luigi_task(task_b)
            assert dbnd_task
            assert len(dbnd_task.task_outputs) == 3
            assert dbnd_task.output1
            assert dbnd_task.output2
            output1 = [
                x for x in dbnd_task.task_meta.task_params if x.name == "output1"
            ][0]
            output2 = [
                x for x in dbnd_task.task_meta.task_params if x.name == "output2"
            ][0]
            assert output1.parameter.kind == _ParameterKind.task_output
            assert output2.parameter.kind == _ParameterKind.task_output


# TODO: Convert to integration test
# def test_postgres(self):
#     t = MyPostgresQuery()
#     ptarget = t.output()
#     ptarget.create_marker_table()
#     with dbnd_config({CoreConfig.databand_url: "http://localhost:8080"}):
#         # with dbnd_config({CoreConfig.tracker: ["file", "console"]}):
#         result = dbnd_luigi_build(tasks=[t])
#     assert result.status == LuigiStatusCode.SUCCESS
