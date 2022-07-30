# © Copyright Databand.ai, an IBM Company 2022

import logging

from functools import partial

from dbnd import output, parameter, task
from dbnd._core.task.decorated_callable_task import DecoratedPythonTask
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


class MyExpTask(DecoratedPythonTask):
    custom_name = parameter.value("aa")

    previous_exp = parameter.value(1)
    score_card = output.csv.data
    my_ratio = output.csv.data

    def run(self):
        # wrapping code
        score = self._invoke_func()

        self.score_card.write(str(score))
        self.my_ratio.write_pickle(self.previous_exp + 1)


my_experiment = partial(task, _task_type=MyExpTask)


class TestCustomkDecorators(TargetTestBase):
    def test_custom_decorator_usage(self):
        @my_experiment
        def run_splits(previous_exp=1):
            logging.warning("Running splits!!!  %s", previous_exp)
            return 1, 2, 1

        @my_experiment
        def my_experiement(alpha=0.2, previous_exp=1):
            logging.warning("My previous exp = %s", previous_exp)

            logging.warning(" Running some splits")

            t = run_splits.t()
            t.dbnd_run()

            logging.warning(" Done some splits")

            return 1, 2, t.result.read_pickle()

        my_exp = my_experiement.t(alpha=0.4)

        # wee can't support creating same task under different dags
        # for the second time - dag will not be added to task
        # my_exp2 = my_experiement.t(previous_exp=my_exp.my_ratio)
        assert_run_task(my_exp)
