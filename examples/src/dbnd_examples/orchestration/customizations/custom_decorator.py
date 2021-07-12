import logging

from functools import partial

from dbnd import output, parameter, task
from dbnd._core.task.decorated_callable_task import DecoratedPythonTask


class ExperiementTask(DecoratedPythonTask):
    custom_name = parameter.value("aa")

    previous_exp = parameter.value(1)
    score_card = output.csv.data
    my_ratio = output.csv.data

    def run(self):
        # wrapping code
        score = self._invoke_func()

        self.score_card.write(str(score))
        self.my_ratio.write_pickle(self.previous_exp + 1)


experiment = partial(task, _task_type=ExperiementTask)


@experiment
def my_new_experiement(alpha: float = 0.2, previous_exp=1):
    logging.warning("My previous exp = %s", previous_exp)
    return 1, 2, alpha
