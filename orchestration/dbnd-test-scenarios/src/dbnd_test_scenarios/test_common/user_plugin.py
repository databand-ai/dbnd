# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging

import dbnd


@contextlib.contextmanager
def my_task_run_context(task_run):
    logging.error("BEFORE %s", task_run)
    yield
    logging.error("AFTER")


@dbnd.hookimpl
def dbnd_task_run_context(task_run):
    return my_task_run_context(task_run)
