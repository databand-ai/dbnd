import datetime

from dbnd import dbnd_config, dbnd_run_cmd, parameter, task


@task
def task_auto_config(
    param_int=parameter[int], param_datetime=parameter[datetime.datetime]
):
    # param_int and param_datetime should be configured by Autoloadconfig
    assert param_int == 42

    from test_dbnd.orchestration.config.autoloaded_config import AutoloadedConfig

    return AutoloadedConfig().param_datetime


class TestBuildErrorsDecorators(object):
    def test_auto_load(self):
        with dbnd_config(
            {
                "autotestconfig": {"param_datetime": "2018-01-01", "param_int": "42"},
                "core": {
                    "user_configs": "autotestconfig",
                    "user_init": "test_dbnd.orchestration.config.autoloaded_config.user_code_load_config",
                },
                "databand": {
                    "module": "test_dbnd.orchestration.config.autoloaded_config"
                },
            }
        ):
            dbnd_run_cmd("task_auto_config")
