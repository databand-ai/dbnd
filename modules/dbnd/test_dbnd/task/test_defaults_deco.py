import datetime
import json
import logging

from dbnd import dbnd_run_cmd, output, parameter, task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets import Target


logger = logging.getLogger(__name__)


class TestTaskDecoratorDefaults(TargetTestBase):
    def test_simple_defaults(self):
        @task
        def t_f_defaults(a=5):
            assert a == 5

        t_f_defaults()
        assert_run_task(t_f_defaults.t())

    def test_simple_no_call(self):
        @task
        def t_f_nocall(a=5):
            assert a == 6

        t_f_nocall(a=6)
        assert_run_task(t_f_nocall.t(a=6))

    def test_simple_with_call(self):
        @task()
        def t_f_call(a=5):
            assert a == 6

        t_f_call(a=6)
        assert_run_task(t_f_call.t(a=6))

    def test_type_hints_from_defaults_cmdline(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        @task
        def t_f_defaults_cmdline(
            a_str="",
            b_datetime=datetime.datetime.utcnow(),
            c_timedelta=datetime.timedelta(),
            d_int=0,
        ):
            assert a_str == "1"
            assert b_datetime.isoformat() == "2018-01-01T10:10:10.100000+00:00"
            assert c_timedelta == datetime.timedelta(days=5)
            assert d_int == 1
            return pandas_data_frame

        dbnd_run_cmd(
            [
                "t_f_defaults_cmdline",
                "-r",
                "a_str=1",
                "-r",
                "b_datetime=2018-01-01T101010.1",
                "-r",
                "c_timedelta=5d",
                "--set",
                json.dumps({"t_f_defaults_cmdline": {"d_int": 1}}),
            ]
        )

    def test_definition_inplace_param(self):
        @task
        def t_f_call(a=parameter[int]):
            assert a == 6

        t_f_call(a=6)
        assert_run_task(t_f_call.t(a=6))

    def test_definition_inplace_output(self):
        @task
        def t_f_call(a=parameter[int], f_output=output[Target]):
            f_output.write(str(a))
            return None

        assert_run_task(t_f_call.t(a=6))
