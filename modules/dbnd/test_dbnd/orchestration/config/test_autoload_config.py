import datetime

from dbnd import new_dbnd_context, parameter, task
from dbnd._core.context.databand_context import DatabandContext


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
        with new_dbnd_context(
            conf={
                "autotestconfig": {"param_datetime": "2018-01-01", "param_int": "42"},
                "core": {
                    "user_configs": "autotestconfig",
                    "user_init": "test_dbnd.orchestration.config.autoloaded_config.user_code_load_config",
                },
                "databand": {
                    "module": "test_dbnd.orchestration.config.autoloaded_config"
                },
            }
        ) as dc:  # type: DatabandContext
            dc.dbnd_run_task(task_or_task_name="task_auto_config")
