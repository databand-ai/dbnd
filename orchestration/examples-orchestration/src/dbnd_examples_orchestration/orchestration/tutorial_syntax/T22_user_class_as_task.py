# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import time

from dbnd import parameter, pipeline, task


logger = logging.getLogger(__name__)


@task
class UserActionObject(object):
    def __init__(self, x_input="x", sleep_period=parameter[datetime.timedelta]):
        self.x_input = x_input
        self.sleep_period = sleep_period

    def run(self):
        logger.info(
            "Running  %s, %s -> UserActionObject", self.x_input, self.sleep_period
        )
        if self.x_input == "ha":
            raise Exception()
        if self.x_input == "sleep":
            time.sleep(self.sleep_period.total_seconds())
        return "{} -> operation_x".format(self.x_input)

    def on_kill(self):
        logger.error("Running on_kill at %s..", self)


@pipeline
def pipe_user_actions(pipe_argument="pipe"):
    step_1 = UserActionObject(x_input=pipe_argument)
    step_2 = UserActionObject(x_input=step_1)

    return step_2


if __name__ == "__main__":
    pipe_user_actions.dbnd_run()
