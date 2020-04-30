import logging

from time import sleep

from dbnd import PipelineTask, band, output, parameter, pipeline, task
from dbnd.tasks.basics import SimplestTask
from dbnd.tasks.basics.simplest import SimpleTask
from dbnd_test_scenarios.test_common.task.factories import TTask
from targets import target


def raise_failure(failure):
    if failure == "missing_params":
        return TTaskMissingParamsMultiple()
    elif failure == "read_error":
        return target("not_exists").read()
    else:
        raise Exception("just an error")


def raise_2(failure):
    raise_failure(failure)


def raise_3(failure):
    raise_2(failure)


class ETaskFails(SimplestTask):
    def run(self):
        raise TypeError("Some user error")


class EPipeWithTaskFails(PipelineTask):
    def band(self):
        return ETaskFails()


class ENoAssignedOutput(PipelineTask):
    t1 = output
    t2 = output
    t_pipeline = output

    def band(self):
        self.t1 = SimplestTask()
        # t2 is missing
        # self.t2 = SimplestTask()


class EWrongOutputAssignment(PipelineTask):
    t1 = output

    def band(self):
        # we should assign Task instances only or their outputs
        self.t1 = ENoAssignedOutput


class EBandWithError(PipelineTask):
    t1 = output
    t2 = output

    def band(self):
        raise Exception("User exception in band method")


@pipeline
def e_band_raise():
    raise Exception("User exception in band method")


@task
def e_task_fails():
    raise_3(None)
    # raise Exception("User exception in band method")


@pipeline
def e_wrong_task_constructor():
    return SimplestTask(not_existing_param=1)


@task
def e_task_with_kwargs(a=2, **kwargs):
    # (int, **Any) -> int
    logging.info("KWARGS: %s", kwargs)
    return a


@pipeline
def e_task_with_kwargs_pipeline():
    return e_task_with_kwargs(10, kwarg=1)


class TErrorRunTask(TTask):
    def run(self):
        raise TypeError("Some user error")


class TError2RunTask(TTask):
    def run(self):
        raise Exception("Some user error")


class TLongTimeRunning(TTask):
    sleep = parameter.value(default=0)

    def run(self):
        if self.sleep:
            sleep(self.sleep)
        super(TLongTimeRunning, self).run()
        raise Exception("Some user error")


class TNestedPipeline(PipelineTask):
    long_time_run = output

    def band(self):
        self.long_time_run = TLongTimeRunning().simplest_output


class TPipeWithErrors(PipelineTask):
    t1 = output
    t2 = output
    t_pipeline = output

    def band(self):
        self.t1 = TErrorRunTask()
        self.t2 = TError2RunTask()
        self.t_pipeline = TNestedPipeline().long_time_run


class TPipelineWrongAssignment(PipelineTask):
    some_output = output

    def band(self):
        self.some_output = PipelineTask


class TTaskMissingParamsMultiple(TTask):
    p1 = parameter[int]
    p2 = parameter[int]
    p3 = parameter[int]


@band
def pipe_bad_band(failure="missing_params"):
    if failure == "missing_params":
        return TTaskMissingParamsMultiple()
    elif failure == "read_error":
        return target("not_exists").read()

    elif failure == "task_run":
        return TErrorRunTask()
    else:
        raise Exception("just an error")


@band
def pipe_bad_band2():
    return pipe_bad_band()


@band
def pipe_bad_band3():
    return pipe_bad_band2()


class TaskBadBand1(PipelineTask):
    failure = parameter[str]

    def band(self):
        raise_failure(self.failure)


class TaskBadBand2(PipelineTask):
    def band(self):
        return TaskBadBand1()


class TaskBadBand3(PipelineTask):
    def band(self):
        return TaskBadBand2()


@pipeline
def pipe_with_task_with_params():
    return SimpleTask()


@pipeline
def p2_with_task_with_params():
    return pipe_with_task_with_params()
