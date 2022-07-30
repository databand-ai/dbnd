# © Copyright Databand.ai, an IBM Company 2022

from dbnd import task
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task_ctrl.task_visualiser import TaskVisualiser
from dbnd._core.tracking.tracking_info_convertor import build_task_run_info


@task
def t_f(input_=parameter[str](hidden=True)):
    print("testing...")
    return "random string"


class TestHiddenParameter(object):
    def test_parameter_value_stays_hidden_in_task_run_info_object(self):
        my_task = t_f.t("test_string")
        task_run = my_task.dbnd_run().root_task_run

        task_run_info = build_task_run_info(task_run)

        input_task_param_info = [
            task_param_info
            for task_param_info in task_run_info.task_run_params
            if task_param_info.parameter_name == "input_"
        ]
        assert len(input_task_param_info) == 1
        assert input_task_param_info[0].value == "***"

    def test_parameter_value_stays_hidden_in_banner(self):
        my_task = t_f.t("test_string")
        my_task.dbnd_run()

        banner = TaskVisualiser(my_task).banner("some_msg")
        print(banner)
        assert "***" in banner and "test_string" not in banner
