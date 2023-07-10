# © Copyright Databand.ai, an IBM Company 2022

import logging

from functools import partial

from dbnd import current_task, parameter, task
from dbnd._core.task.decorated_callable_task import DecoratedPythonTask
from targets.values import ValueType, register_value_type


# INFRA OBJECTS
class MyConfig(object):
    def __init__(self, config_id, config_ratio):
        self.config_id = config_id
        self.config_ratio = config_ratio

    def __str__(self):
        return "CONFIG: %s, %s" % (self.config_id, self.config_ratio)


class MyConfigTypeHandler(ValueType):
    type = MyConfig

    def parse_from_str(self, x):
        logging.info("Loading config %s from DB..", x)
        return MyConfig(config_id=x, config_ratio=0.7)

    def to_str(self, x):
        return str(x)

    def to_signature(self, x):
        return x.config_id


register_value_type(MyConfigTypeHandler())


class MyProjectTask(DecoratedPythonTask):
    config = parameter[MyConfig]

    def should_rerun(self):
        return self.config.config_ratio == 0.2

    def on_kill(self):
        pass


my_project_task = partial(task, _task_type=MyProjectTask)


# USAGE EXAMPLE
@my_project_task
def my_new_experiement(alpha: float = 0.2, config=parameter[MyConfig]):
    logging.warning("My current config= %s", config)
    return 1, 2, alpha


@my_project_task
class MyAnotherExperiement(object):
    def __init__(self, config, ratio=0.2):
        self.ratio = ratio
        logging.info("Creating MyAnotherExperiement with config: %s", config)
        self.config_id = config.config_id
        super(MyAnotherExperiement, self).__init__()

    def run(self):
        # current config
        t = current_task()

        logging.info("Config : %s", t.config)
        logging.info("Ratio : %s", self.ratio)
